package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WriteAheadLog {
    public static final Logger LOG = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final AtomicInteger nextGen = new AtomicInteger();
    static String dir;

    final int gen;
    private final LinkedBlockingQueue<Object> writeQueue;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicInteger totalBytes = new AtomicInteger();
    private final Thread writerThread;

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

    private void start() {
        writerThread.start();
    }

    private WriteAheadLog(int gen) {
        this.gen = gen;
        LOG.info("create {}", walFileName());
        this.writeQueue = new LinkedBlockingQueue<>(1000);
        this.writerThread = new WriterThread("wal" + gen + "-writer");
    }

    public void stop() {
        stop.set(true);
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
        LOG.debug("append {} to write-queue {}", cmd.key, walFileName());
        boolean done = false;
        int attempt = 0;
        do {
            if (stop.get()) {
                throw new IllegalStateException("wal stopped, append not allowed");
            }
            try {
                attempt += 1;
                done = writeQueue.offer(cmd, 10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.error("retrying append to queue for {}, attempts: {}, exception: {}", cmd.key, attempt, e);
            }
        } while (!done);
        LOG.debug("done... append {} to write-queue {} after {} attempts", cmd.key, walFileName(), attempt);
    }

    @Override
    public String toString() {
        return walFileName();
    }

    private class WriterThread extends Thread {
        public WriterThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            LOG.info("wal writer thread started - {}", getName());

            FileOutputStream fos;
            DataOutputStream os;
            try {
                fos = new FileOutputStream(walFileName(), true);
                os = new DataOutputStream(new BufferedOutputStream(fos, 1024 * 8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            int count = 0;
            int errors = 0;
            while (true) {
                if (stop.get() && writeQueue.isEmpty()) {
                    break;
                }
                try {
                    Object cmd = writeQueue.take();
                    if (cmd instanceof SetCmd) {
                        SetCmd setCmd = (SetCmd) cmd;
                        LOG.debug("appending SetCmd key: {}", setCmd.key);
                        KeyValueEntry entry = new KeyValueEntry((byte) CmdType.Set.code, setCmd.key, setCmd.value);
                        entry.writeTo(os);
                        totalBytes.addAndGet(entry.totalLength());
                    } else {
                        errors += 1;
                        LOG.error("unrecognized command: " + cmd);
                    }
                    if ((count += 1) % 10000 == 0) {
                        LOG.info("processed {}, errors {}, queue-length {}", count, errors, writeQueue.size());
                    }
                } catch (InterruptedException e) {
                    LOG.error("caught exception in queue.take, retrying: " + e);
                } catch (IOException e) {
                    LOG.error("caught IOException, exiting: ", e);
                    break;
                }
            }
            try {
                os.close();
            } catch (IOException e) {
                LOG.error("caught IOException when closing output stream, ignoring ", e);

            }
            LOG.info("wal writer thread exiting - {}", getName());
        }
    }

}













