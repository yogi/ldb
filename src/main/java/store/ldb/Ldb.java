package store.ldb;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.Store;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Ldb implements Store {
    public static final Logger LOG = LoggerFactory.getLogger(Ldb.class);

    private final String dir;
    private final TreeMap<Integer, Level> levels;
    private final AtomicBoolean writeSegmentInProgress = new AtomicBoolean(false);
    public final Config config;
    private final Compactor compactor;
    private volatile TreeMap<String, String> memtable;
    private volatile WriteAheadLog wal;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Throttler throttler;

    public Ldb(String dir) {
        this(dir, Config.defaultConfig());
    }

    public Ldb(String dir, Config config) {
        this.config = config;
        this.dir = dir;
        this.levels = Level.loadLevels(dir, config);
        this.wal = WriteAheadLog.init(dir, levels.get(0));
        this.compactor = new Compactor(levels, config);
        this.memtable = new TreeMap<>();
        this.throttler = new Throttler();
        Segment.resetCache(config.segmentCacheSize);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public void startCompactor() {
        compactor.start();
    }

    public void stop() {
        LOG.info("stop");
        wal.stop();
        compactor.stop();
    }

    public void set(String key, String value) {
        assertKeySize(key);
        assertValueSize(value);
        key = randomize(key);
        throttler.throttle();
        lock.writeLock().lock();
        try {
            wal.append(new SetCmd(key, value));
            memtable.put(key, value);
            writeSegmentIfNeeded();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String randomize(String key) {
        return config.randomizedKeys ? DigestUtils.sha256Hex(key) : key;
    }

    public Optional<String> get(String key) {
        assertKeySize(key);
        key = randomize(key);
        lock.readLock().lock();
        try {
            if (memtable.containsKey(key)) {
                LOG.debug("get found {} in memtable", key);
                return Optional.of(memtable.get(key));
            }
            for (Level level : levels.values()) {
                Optional<String> value = level.get(key);
                if (value.isPresent()) {
                    return value;
                }
            }
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeSegmentIfNeeded() {
        if (wal.totalBytes() >= config.maxWalSize) {
            flushMemtable();
        }
    }

    /**
     * Package access only for tests, should not be called without a lock
     */
    void flushMemtable() {
        if (writeSegmentInProgress.compareAndExchange(false, true)) return;

        WriteAheadLog oldWal = wal;
        TreeMap<String, String> oldMemtable = memtable;

        LOG.debug("wal threshold crossed, init new wal and memtable before flushing old one {}", oldWal);
        wal = WriteAheadLog.startNext();
        memtable = new TreeMap<>();

        LOG.debug("flush segment from memtable for wal {}", oldWal);
        try {
            oldWal.stop();
            levels.get(0).flushMemtable(oldMemtable);
            oldWal.delete();
        } finally {
            writeSegmentInProgress.set(false);
        }
    }

    @Override
    public String stats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("memtable.size", memtable.size());
        stats.put("db.keys", levels.values().stream().mapToLong(Level::keyCount).sum());
        stats.put("db.totalBytes", levels.values().stream().mapToLong(Level::totalBytes).sum());
        stats.put("segmentCache", Segment.cacheStats());
        levels.values().forEach(level -> {
            level.addStats(stats);
        });
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

    private class Throttler {
        public static final double THRESHOLD = 1.5;

        private final AtomicBoolean throttling = new AtomicBoolean();
        private final AtomicLong lastChecked = new AtomicLong();

        public void throttle() {
            if (throttling.get()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            if (System.currentTimeMillis() - lastChecked.get() > 1000) {
                final double score = levels.get(0).getCompactionScore();
                lastChecked.set(System.currentTimeMillis());
                if (score <= THRESHOLD && throttling.compareAndExchange(true, false)) {
                    LOG.info("stop throttling, level0 score: {}", score);
                } else if (score > THRESHOLD && !throttling.compareAndExchange(false, true)) {
                    LOG.info("start throttling, level0 score: {}", score);
                }
            }
        }
    }
}
