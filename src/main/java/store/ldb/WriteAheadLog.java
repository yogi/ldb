package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WriteAheadLog {
    public static final Logger LOG = LoggerFactory.getLogger(WriteAheadLog.class);

    private final String dir;
    private final Manifest manifest;
    private final int gen;
    private final DataOutputStream os;
    private final AtomicInteger totalBytes = new AtomicInteger();

    private enum CmdType {
        Set(1),
        ;
        final int code;

        CmdType(int code) {
            this.code = code;
        }
    }

    static void replayExistingOnStartup(String dir, Level levelZero, Manifest manifest) {
        LinkedList<WriteAheadLog> wals =
                Arrays.stream(Objects.requireNonNull(new File(dir)
                                .listFiles((dir1, name) -> name.startsWith("wal"))))
                        .map(file -> Integer.parseInt(file.getName().replace("wal", "")))
                        .sorted()
                        .map((Integer gen) -> new WriteAheadLog(gen, dir, manifest))
                        .collect(Collectors.toCollection(LinkedList::new));
        if (wals.isEmpty()) return;
        Memtable memtable = new Memtable();
        wals.forEach(wal -> wal.replay(memtable));
        flushAndDelete(wals, memtable, levelZero, manifest);
    }

    static void flushAndDelete(Collection<WriteAheadLog> wals, Memtable memtable, Level levelZero, Manifest manifest) {
        wals.forEach(WriteAheadLog::stop);
        if (memtable.size() > 0) {
            List<Segment> segmentsCreated = levelZero.flushMemtable(memtable);
            manifest.record(segmentsCreated, List.of());
        }
        wals.forEach(WriteAheadLog::delete);
    }

    WriteAheadLog(Integer gen, String dirname, Manifest manifest) {
        try {
            this.gen = gen;
            this.dir = dirname;
            this.manifest = manifest;
            this.os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(walFileName(), true), 1024 * 8));
            LOG.debug("create {}", walFileName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            if (os != null) os.close();
        } catch (IOException e) {
            LOG.error("caught IOException when closing output stream, ignoring ", e);
        }
    }

    public void delete() {
        LOG.debug("delete {}", walFileName());
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

    public WriteAheadLog startNext() {
        return new WriteAheadLog(gen + 1, dir, manifest);
    }

    private String walFileName() {
        return dir + File.separatorChar + "wal" + this.gen;
    }

    void replay(Memtable memtable) {
        File file = new File(walFileName());
        if (!file.exists() || file.length() == 0) {
            return;
        }
        int count = 0;
        long start = System.currentTimeMillis();
        try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(walFileName())))) {
            while (is.available() > 0) {
                KeyValueEntry entry = KeyValueEntry.readFrom(is);
                memtable.put(entry.getKey(), entry.getValue());
                count += 1;
                if (count % 100000 == 0) {
                    LOG.debug("replayed from wal: {}", count);
                }
            }
            LOG.debug("replayed from wal: {} keys, store-size: {}, in {} ms", count, memtable.size(), (System.currentTimeMillis() - start));
        } catch (IOException e) {
            LOG.info("ignoring error replaying wal on startup: " + e.getMessage());
        }
    }

    public void append(SetCmd cmd) {
        try {
            LOG.trace("append {} to {}", cmd.key, walFileName());
            KeyValueEntry entry = new KeyValueEntry((byte) CmdType.Set.code, cmd.key, cmd.value);
            entry.writeTo(os);
            totalBytes.addAndGet(entry.totalBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return walFileName();
    }

}













