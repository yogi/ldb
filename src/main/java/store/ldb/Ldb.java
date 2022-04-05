package store.ldb;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.Store;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Ldb implements Store {
    public static final Logger LOG = LoggerFactory.getLogger(Ldb.class);

    private final Manifest manifest;
    private final String dir;
    private final Levels levels;
    public final Config config;
    private final Compactor compactor;
    private volatile Memtable memtable;
    private volatile Memtable oldMemtable;
    private volatile WriteAheadLog wal;
    private volatile WriteAheadLog oldWal;
    private final Throttler throttler;

    public Ldb(String dir) {
        this(dir, Config.defaultConfig());
    }

    public Ldb(String dir, Config config) {
        this.config = config;
        this.dir = dir;
        this.manifest = new Manifest(dir);
        this.levels = new Levels(dir, config, manifest);
        WriteAheadLog.replayExistingOnStartup(dir, levels.levelZero(), manifest);
        this.wal = new WriteAheadLog(0, dir, manifest);
        this.compactor = new Compactor(levels, config, manifest);
        this.memtable = new Memtable();
        this.throttler = new Throttler(config, () -> levels.getCompactionScore() > 2);
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
            oldWal = wal;
            oldMemtable = memtable;

            LOG.debug("wal threshold crossed, init new wal and memtable before flushing old one {}", oldWal);
            wal = wal.startNext();
            memtable = new Memtable();

            LOG.debug("flush segment from memtable for wal {}", oldWal);
            WriteAheadLog.flushAndDelete(List.of(oldWal), oldMemtable, levels.levelZero(), manifest);

            oldWal = null;
            oldMemtable = null;
        }
    }

    private String randomize(String key) {
        return config.randomizedKeys ? DigestUtils.sha256Hex(key) : key;
    }

    public Optional<String> get(String key) {
        assertKeySize(key);
        key = randomize(key);

        // check memtable first
        if (memtable.contains(key)) {
            LOG.debug("get found {} in memtable", key);
            return Optional.of(memtable.get(key));
        }

        // check the old memtable if a flush is in progress
        try {
            if (oldMemtable != null && oldMemtable.contains(key)) {
                LOG.debug("get found {} in old memtable", key);
                return Optional.of(oldMemtable.get(key));
            }
        } catch (NullPointerException e) {
            // ignore this exception, it can happen if oldMemtable is set to null after the null-check but before the contains() call
            LOG.info("safely ignoring NPE in get() because oldMemtable was null");
        }

        // ask the levels finally
        final ByteBuffer keyBuf = ByteBuffer.wrap(key.getBytes());
        return levels.getValue(key, keyBuf);
    }

    @Override
    public String stats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("memtable.size", memtable.size());
        stats.put("db.keys", levels.keyCount());
        stats.put("db.totalBytes", levels.totalBytes());
        stats.put("segmentCache", Segment.cacheStats());
        levels.addStats(stats);
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

}
