package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WriteAheadLog {
    public static final Logger LOG = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final AtomicInteger nextGen = new AtomicInteger();
    static String dir;

    final int gen;
    private DataOutputStream os;
    private final AtomicInteger totalBytes = new AtomicInteger();

    private enum CmdType {
        Set(1),
        ;
        final int code;

        CmdType(int code) {
            this.code = code;
        }
    }

    public static WriteAheadLog init(String dirname, Level levelZero) {
        dir = dirname;
        int maxGen = replayExistingOnStartup(levelZero);
        nextGen.set(maxGen == 0 ? 0 : maxGen + 1);
        return startNext();
    }

    private static int replayExistingOnStartup(Level levelZero) {
        LinkedList<WriteAheadLog> wals =
                Arrays.stream(Objects.requireNonNull(new File(dir)
                                .listFiles((dir1, name) -> name.startsWith("wal"))))
                        .map(file -> Integer.parseInt(file.getName().replace("wal", "")))
                        .sorted()
                        .map(WriteAheadLog::new)
                        .collect(Collectors.toCollection(LinkedList::new));
        if (wals.isEmpty()) {
            return 0;
        }
        TreeMap<String, String> memtable = new TreeMap<>();
        wals.forEach(wal -> wal.replay(memtable));
        if (memtable.size() > 0) {
            levelZero.flushMemtable(memtable);
        }
        wals.forEach(WriteAheadLog::delete);
        return wals.getLast().gen;
    }

    private WriteAheadLog(int gen) {
        this.gen = gen;
        LOG.info("create {}", walFileName());
    }

    private void start() {
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(walFileName(), true);
            this.os = new DataOutputStream(new BufferedOutputStream(fos, 1024 * 8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            os.close();
        } catch (IOException e) {
            LOG.error("caught IOException when closing output stream, ignoring ", e);
        }
    }

    public void delete() {
        LOG.info("delete {}", walFileName());
        File file = new File(walFileName());
        if (file.exists()) {
            if (!file.delete()) {
                throw new RuntimeException("could not delete wal: " + walFileName());
            }
        }
    }

    public long totalBytes() {
        return totalBytes.get();
    }

    public static WriteAheadLog startNext() {
        final WriteAheadLog wal = new WriteAheadLog(nextGen.getAndIncrement());
        wal.start();
        return wal;
    }

    private String walFileName() {
        return dir + File.separatorChar + "wal" + gen;
    }

    void replay(SortedMap<String, String> memtable) {
        File file = new File(walFileName());
        if (!file.exists() || file.length() == 0) {
            return;
        }
        int count = 0;
        long start = System.currentTimeMillis();
        try {
            DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(walFileName())));
            while (is.available() > 0) {
                KeyValueEntry entry = KeyValueEntry.readFrom(is);
                memtable.put(entry.key, entry.value);
                count += 1;
                if (count % 100000 == 0) {
                    LOG.debug("replayed from wal: {}", count);
                }
            }
            LOG.debug("replayed from wal: {} keys, store-size: {}, in {} ms", count, memtable.size(), (System.currentTimeMillis() - start));
            is.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(SetCmd cmd) {
        try {
            LOG.debug("append {} to {}", cmd.key, walFileName());
            KeyValueEntry entry = new KeyValueEntry((byte) CmdType.Set.code, cmd.key, cmd.value);
            entry.writeTo(os);
            totalBytes.addAndGet(entry.totalLength());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return walFileName();
    }

}













