package store.ldb;

import store.Store;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class LDB implements Store {
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
        wal.append(new SetCmd(key, value));
        memtable.put(key, value);
        writeSegmentIfNeeded();
    }

    public synchronized Optional<String> get(String key) {
        if (memtable.containsKey(key)) {
            return Optional.of(memtable.get(key));
        }
        for (Level level : levels.values()) {
            Optional<String> value = level.get(key);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    private void writeSegmentIfNeeded() {
        if (walThresholdCrossed(wal) && !writeSegmentInProgress.get()) {
            writeSegmentInProgress.set(true);

            WriteAheadLog oldWal = wal;
            TreeMap<String, String> oldMemtable = memtable;

            wal = oldWal.createNext();
            memtable = new TreeMap<>();

            new Thread(() -> {
                try {
                    oldWal.stop();
                    levels.get(0).addSegment(oldMemtable);
                    oldWal.delete();
                } finally {
                    writeSegmentInProgress.set(false);
                }
            }).start();
        }
    }

    private boolean walThresholdCrossed(WriteAheadLog wal) {
        return wal.fileSize() > 100 * 1024 * 1024;
    }

    @Override
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("memtable.size", memtable.size());
        stats.put("db.keys", levels.values().stream().mapToLong(Level::keyCount).sum());
        return stats;
    }

}
