package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.Store;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class LDB implements Store {
    public static final Logger LOG = LoggerFactory.getLogger(LDB.class);

    private final String dir;
    private final TreeMap<Integer, Level> levels;
    private final AtomicBoolean writeSegmentInProgress = new AtomicBoolean(false);
    private volatile TreeMap<String, String> memtable;
    private volatile WriteAheadLog wal;

    public LDB(String dir) {
        this.dir = dir;
        this.levels = Level.loadLevels(dir);
        this.wal = WriteAheadLog.init(dir, levels.get(0));
        this.memtable = new TreeMap<>();
    }

    public synchronized void set(String key, String value) {
        LOG.debug("set {}", key);
        wal.append(new SetCmd(key, value));
        memtable.put(key, value);
        writeSegmentIfNeeded();
    }

    public synchronized Optional<String> get(String key) {
        if (memtable.containsKey(key)) {
            LOG.debug("get found {} in memtable", key);
            return Optional.of(memtable.get(key));
        }
        for (Level level : levels.values()) {
            Optional<String> value = level.get(key);
            if (value.isPresent()) {
                LOG.debug("get found {} in level", level.dirPathName());
                return value;
            }
        }
        LOG.debug("get did not find {}", key);
        return Optional.empty();
    }

    private void writeSegmentIfNeeded() {
        if (walThresholdCrossed(wal) && !writeSegmentInProgress.get()) {
            writeSegmentInProgress.set(true);

            WriteAheadLog oldWal = wal;
            TreeMap<String, String> oldMemtable = memtable;

            wal = oldWal.createNext();
            memtable = new TreeMap<>();

            try {
                oldWal.stop();
                levels.get(0).addSegment(oldMemtable);
                oldWal.delete();
            } finally {
                writeSegmentInProgress.set(false);
            }
        }
    }

    private boolean walThresholdCrossed(WriteAheadLog wal) {
        return wal.fileSize() > 10 * 1024 * 1024;
    }

    @Override
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("memtable.size", memtable.size());
        stats.put("db.keys", levels.values().stream().mapToLong(Level::keyCount).sum());
        return stats;
    }

}
