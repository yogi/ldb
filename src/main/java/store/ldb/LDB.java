package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.Store;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LDB implements Store {
    public static final Logger LOG = LoggerFactory.getLogger(LDB.class);
    public static final int MB = 1024 * 1025;
    public static final int DEFAULT_MAX_SEGMENT_SIZE = 10 * MB;
    public static final int DEFAULT_MIN_COMPACTION_SEGMENT_COUNT = 5;
    public static final int DEFAULT_NUM_LEVELS = 3;

    private final String dir;
    private final TreeMap<Integer, Level> levels;
    private final AtomicBoolean writeSegmentInProgress = new AtomicBoolean(false);
    private final Compactor compactor;
    private volatile TreeMap<String, String> memtable;
    private volatile WriteAheadLog wal;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LDB(String dir) {
        this(dir, DEFAULT_MAX_SEGMENT_SIZE, DEFAULT_MIN_COMPACTION_SEGMENT_COUNT, DEFAULT_NUM_LEVELS);
    }

    public LDB(String dir, int maxSegmentSize, int minCompactionSegmentCount, int numLevels) {
        this.dir = dir;
        this.levels = Level.loadLevels(dir, maxSegmentSize, numLevels);
        this.wal = WriteAheadLog.init(dir, levels.get(0));
        this.compactor = Compactor.start(levels, minCompactionSegmentCount);
        this.memtable = new TreeMap<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutting down");
            wal.stop();
            compactor.stop();
        }));
    }

    public void set(String key, String value) {
        lock.writeLock().lock();
        try {
            LOG.debug("set {}", key);
            wal.append(new SetCmd(key, value));
            memtable.put(key, value);
            writeSegmentIfNeeded();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<String> get(String key) {
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
            LOG.debug("get did not find {}", key);
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeSegmentIfNeeded() {
        if (walThresholdCrossed(wal) && !writeSegmentInProgress.get()) {
            writeSegmentInProgress.set(true);

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
    }

    private boolean walThresholdCrossed(WriteAheadLog wal) {
        return wal.totalBytes() > levels.firstEntry().getValue().maxSegmentSize();
    }

    @Override
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("memtable.size", memtable.size());
        stats.put("db.keys", levels.values().stream().mapToLong(Level::keyCount).sum());
        stats.put("db.totalBytes", levels.values().stream().mapToLong(Level::totalBytes).sum());
        levels.values().forEach(level -> {
            stats.put(level.dirPathName() + ".segments", level.segmentCount());
            stats.put(level.dirPathName() + ".keyCount", level.keyCount());
            stats.put(level.dirPathName() + ".totalBytes", level.totalBytes());
        });
        return stats;
    }

    public void pauseCompactor() {
        compactor.pause();
    }

    public void unpauseCompactor() {
        compactor.unpause();
    }

    public void runCompaction(int levelNum) {
        compactor.runCompaction(levelNum);
    }
}
