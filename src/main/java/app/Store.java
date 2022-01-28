package app;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Store {
    public static final int SNAPSHOT_WAL_THRESHOLD = 100000;
    private final AtomicBoolean snapshotInProgress = new AtomicBoolean(false);
    volatile Map<String, String> memtable = new ConcurrentHashMap<>();
    private volatile WriteAheadLog wal;

    public Store(String dir) {
        loadSnapshot(dir + File.separatorChar + "snapshot");

        List<Integer> wals = walGenerations(dir);
        if (wals.isEmpty()) {
            return;
        }

        wal = new WriteAheadLog(dir, wals.get(0));
        wal.replay(this);

        if (wals.size() == 2) {
            wal.delete();
            loadSnapshot(dir + File.separatorChar + "snapshot.new");
            wal = new WriteAheadLog(dir, wals.get(1));
            wal.replay(this);
            replaceOldSnapshot();
        }
    }

    private static List<Integer> walGenerations(String dir) {
        File[] wals = new File(dir).listFiles((dir1, name) -> name.startsWith("wal"));
        if (wals != null && wals.length > 2) {
            throw new IllegalStateException("more than two wals found");
        }
        if (wals == null || wals.length == 0) {
            return List.of(0);
        }
        return Arrays.stream(wals)
                .map(file -> Integer.parseInt(file.getName().replace("wal", "")))
                .sorted()
                .collect(Collectors.toList());
    }

    private void loadSnapshot(String snapshotFileName) {
        File file = new File(snapshotFileName);
        if (!file.exists() || file.length() == 0) {
            return;
        }
        int count = 0;
        try {
            DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(snapshotFileName)));
            while (is.available() > 0) {
                KeyValueEntry entry = KeyValueEntry.readFrom(is);
                setRaw(entry.key, entry.value);
                count += 1;
                if (count % 100000 == 0) {
                    System.out.println("restoring from snapshot: " + count + " store-size: " + memtable.size());
                }
            }
            System.out.println("restored from snapshot: " + count + " store-size: " + memtable.size());
            is.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized String get(String key) {
        return memtable.get(key);
    }

    public synchronized void set(String key, String value) {
        if (!snapshotInProgress.get() && snapshotThresholdCrossed()) {
            snapshot();
        }
        wal.append(new SetCmd(key, value));
        setRaw(key, value);
    }

    private synchronized void snapshot() {
        Map<String, String> oldMemtable = memtable;
        WriteAheadLog oldWal = wal;
        oldWal.stop();
        memtable = new ConcurrentHashMap<>(oldMemtable);
        wal = new WriteAheadLog(oldWal.dir, oldWal.gen + 1);

        new Thread(() -> {
            snapshotInProgress.set(true);
            try {
                writeSnapshot(oldMemtable);
                oldWal.delete();
            } finally {
                snapshotInProgress.set(false);
            }
        }).start();
    }

    private void writeSnapshot(Map<String, String> map) {
        System.out.println("writing snapshot");
        long start = System.currentTimeMillis();
        String newSnapshotFileName = wal.dir + File.separatorChar + "snapshot.new";

        try {
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newSnapshotFileName, true), 1024 * 8));

            int count = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                KeyValueEntry keyValueEntry = new KeyValueEntry((byte) 0, k, v);
                keyValueEntry.writeTo(os);
                count += 1;
            }
            os.close();

            replaceOldSnapshot();

            System.out.printf("writing snapshot... done %d keys in %d ms%n", count, System.currentTimeMillis() - start);
        } catch (IOException e) {
            System.out.printf("writing snapshot... error: %s\n", e);
            throw new RuntimeException(e);
        }
    }

    private void replaceOldSnapshot() {
        File curSnapshot = new File(wal.dir + File.separatorChar + "snapshot");
        if (curSnapshot.exists()) {
            if (!curSnapshot.delete()) {
                throw new IllegalStateException("could not delete old snapshot");
            }
        }
        File newSnapshot = new File(wal.dir + File.separatorChar + "snapshot.new");
        if (newSnapshot.exists() && !newSnapshot.renameTo(curSnapshot)) {
            throw new IllegalStateException("could not rename new snapshot");
        }
    }

    private boolean snapshotThresholdCrossed() {
        return wal.count() > SNAPSHOT_WAL_THRESHOLD;
    }

    void setRaw(String key, String value) {
        memtable.put(key, value);
    }
}
