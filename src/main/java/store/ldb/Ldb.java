package store.ldb;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.Store;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Ldb implements Store {
    public static final Logger LOG = LoggerFactory.getLogger(Ldb.class);

    private final String dir;
    private final TreeMap<Integer, Level> levels;
    private final AtomicBoolean writeSegmentInProgress = new AtomicBoolean(false);
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
        this.levels = Level.loadLevels(dir, config);
        this.wal = WriteAheadLog.init(dir, levels.get(0));
        this.compactor = new Compactor(levels, config);
        this.memtable = new ConcurrentSkipListMap<>();
        this.throttler = new Throttler(() -> levels.get(0).getCompactionScore() > 1.5);
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
            oldWal.stop();
            levels.get(0).flushMemtable(oldMemtable);
            oldWal.delete();
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
        final AtomicBoolean throttling = new AtomicBoolean();
        int sleepDuration = 1;
        private final Supplier<Boolean> thresholdCheck;

        public Throttler(Supplier<Boolean> thresholdCheck) {
            this.thresholdCheck = thresholdCheck;
            Thread thread = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(1000);
                        checkThreshold();
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        void checkThreshold() {
            boolean breached = thresholdCheck.get();
            if (!breached && throttling.get()) {
                if (sleepDuration > 1) {
                    LOG.info("decrease throttling sleep duration to {}", sleepDuration);
                    sleepDuration -= 1;
                } else {
                    LOG.info("stop throttling");
                    throttling.set(false);
                }
            } else if (breached) {
                if (throttling.get()) {
                    LOG.info("increase throttling sleep duration to {}", sleepDuration);
                    sleepDuration = Math.min(sleepDuration + 1, 5);
                } else {
                    LOG.info("start throttling");
                    throttling.set(true);
                }
            }
        }

        public void throttle() {
            if (throttling.get()) {
                try {
                    Thread.sleep(sleepDuration);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }
}
