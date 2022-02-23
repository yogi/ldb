package store.ldb;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.Store;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Ldb implements Store {
    public static final Logger LOG = LoggerFactory.getLogger(Ldb.class);

    private final Manifest manifest;
    private final String dir;
    private final TreeMap<Integer, Level> levels;
    public final Config config;
    private final Compactor compactor;
    private volatile ConcurrentSkipListMap<String, String> memtable;
    private volatile WriteAheadLog wal;
    private final Throttler throttler;

    public Ldb(String dir) {
        this(dir, Config.defaultConfig());
    }

    public Ldb(String dir, Config config) {
        this.config = config;
        this.dir = dir;
        this.manifest = new Manifest(dir);
        this.levels = Level.loadLevels(dir, config, manifest);
        this.wal = WriteAheadLog.init(dir, levels.get(0), manifest);
        this.compactor = new Compactor(levels, config, manifest);
        this.memtable = new ConcurrentSkipListMap<>();
        this.throttler = new Throttler(config, () -> levels.get(0).getCompactionScore() > 2);
        Segment.resetCache(config.segmentCacheSize);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public void startCompactor() {
        compactor.start();
    }

    public synchronized void stop() {
        LOG.info("stop");
        wal.stop();
        compactor.stop();
        manifest.close();
    }

    public synchronized void set(String key, String value) {
        throttler.throttle();
        assertKeySize(key);
        assertValueSize(value);
        key = randomize(key);
        wal.append(new SetCmd(key, value));
        memtable.put(key, value);

        // flush memtable
        if (wal.totalBytes() >= config.maxWalSize) {
            WriteAheadLog oldWal = wal;
            ConcurrentSkipListMap<String, String> oldMemtable = memtable;

            LOG.debug("wal threshold crossed, init new wal and memtable before flushing old one {}", oldWal);
            wal = WriteAheadLog.startNext();
            memtable = new ConcurrentSkipListMap<>();

            LOG.debug("flush segment from memtable for wal {}", oldWal);
            WriteAheadLog.flushAndDelete(List.of(oldWal), oldMemtable, levels.get(0));
        }
    }

    private String randomize(String key) {
        return config.randomizedKeys ? DigestUtils.sha256Hex(key) : key;
    }

    public Optional<String> get(String key) {
        assertKeySize(key);
        key = randomize(key);
        if (memtable.containsKey(key)) {
            LOG.debug("get found {} in memtable", key);
            return Optional.of(memtable.get(key));
        }
        final ByteBuffer keyBuf = ByteBuffer.wrap(key.getBytes());
        for (Level level : levels.values()) {
            Optional<String> value = level.get(key, keyBuf);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    @Override
    public String stats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("memtable.size", memtable.size());
        stats.put("db.keys", levels.values().stream().mapToLong(Level::keyCount).sum());
        stats.put("db.totalBytes", levels.values().stream().mapToLong(Level::totalBytes).sum());
        stats.put("segmentCache", Segment.cacheStats());
        levels.values().forEach(level -> level.addStats(stats));
        return stats.entrySet().stream().map(Object::toString).collect(Collectors.joining("\n"));
    }

    public void runCompaction(int levelNum) {
        compactor.runCompaction(levelNum);
    }

    private void assertValueSize(String value) {
        assertSize(value, KeyValueEntry.MAX_KEY_SIZE, "key");
    }

    private void assertKeySize(String key) {
        assertSize(key, KeyValueEntry.MAX_VALUE_SIZE, "value");
    }

    private void assertSize(String s, int maxSize, String keyOrValue) {
        if (s.length() > maxSize) {
            throw new IllegalArgumentException(format("max %s size supported %d bytes, got %d bytes: %s",
                    keyOrValue, maxSize, s.getBytes().length, s));
        }
    }

    @Override
    public String toString() {
        return "LDB{" +
                "dir='" + dir + '\'' +
                ", levels=" + levels +
                '}';
    }

    static class Throttler {
        public static final int MIN_SLEEP_NANOS = 10000;
        public static final int SLEEP_INCREMENT_NANOS = 10000;
        public static final int MAX_SLEEP_NANOS = 1000000;
        public static final int ONE_MILLI_IN_NANOS = 1000000;
        final AtomicBoolean throttling = new AtomicBoolean();
        final AtomicInteger sleepDurationNanos = new AtomicInteger(MIN_SLEEP_NANOS);
        private final Supplier<Boolean> thresholdCheck;

        public Throttler(Config config, Supplier<Boolean> thresholdCheck) {
            this.thresholdCheck = thresholdCheck;
            if (config.enableThrottling) {
                new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(this::checkThreshold, 0, 1, TimeUnit.SECONDS);
            }
        }

        void checkThreshold() {
            boolean breached = thresholdCheck.get();
            if (!breached && throttling.get()) {
                if (sleepDurationNanos.get() > MIN_SLEEP_NANOS) {
                    sleepDurationNanos.set(sleepDurationNanos.get() - SLEEP_INCREMENT_NANOS);
                    LOG.info("decrease throttling sleep duration to {} ms", sleepDurationInMillis());
                } else {
                    LOG.info("stop throttling");
                    throttling.set(false);
                    sleepDurationNanos.set(MIN_SLEEP_NANOS);
                }
            } else if (breached) {
                if (throttling.get()) {
                    sleepDurationNanos.set(Math.min(sleepDurationNanos.get() + SLEEP_INCREMENT_NANOS, MAX_SLEEP_NANOS));
                    LOG.info("increase throttling sleep duration to {} ms", sleepDurationInMillis());
                } else {
                    LOG.info("start throttling, sleep duration is {} ms", sleepDurationInMillis());
                    throttling.set(true);
                }
            }
        }

        private double sleepDurationInMillis() {
            return sleepDurationNanos.get() / (double) ONE_MILLI_IN_NANOS;
        }

        public void throttle() {
            if (throttling.get()) {
                try {
                    final int nanos = sleepDurationNanos.get();
                    Thread.sleep(nanos / ONE_MILLI_IN_NANOS, nanos % ONE_MILLI_IN_NANOS);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

}
