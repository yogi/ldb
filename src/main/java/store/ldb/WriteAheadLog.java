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

    final int gen;
    final String dir;
    private final DataOutputStream os;
    private final LinkedBlockingQueue<Object> queue;
    private final Thread writerThread;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicInteger count = new AtomicInteger();

    public static WriteAheadLog init(String dir, Level levelZero) {
        int nextGen = 0;
        List<WriteAheadLog> wals = loadWals(dir);
        if (!wals.isEmpty()) {
            TreeMap<String, String> map = new TreeMap<>();
            wals.forEach(wal -> wal.replay(map));
            if (map.size() > 0) {
                levelZero.addSegment(map);
                wals.forEach(WriteAheadLog::delete);
                nextGen = wals.get(wals.size() - 1).gen + 1;
            }
        }
        return new WriteAheadLog(dir, nextGen);
    }

    public static List<WriteAheadLog> loadWals(String dir) {
        return Arrays.stream(Objects.requireNonNull(new File(dir)
                        .listFiles((dir1, name) -> name.startsWith("wal"))))
                .map(file -> Integer.parseInt(file.getName().replace("wal", "")))
                .sorted()
                .map(gen -> new WriteAheadLog(dir, gen))
                .collect(Collectors.toList());
    }

    public int count() {
        return count.get();
    }

    public void stop() {
        stop.set(true);
    }

    public void delete() {
        File file = new File(walFileName());
        if (file.exists()) {
            file.delete();
        }
    }

    public Integer gen() {
        return gen;
    }

    public long fileSize() {
        return new File(walFileName()).length();
    }

    public WriteAheadLog createNext() {
        return new WriteAheadLog(dir, gen + 1);
    }

    private enum CmdType {
        Set,
    }

    public WriteAheadLog(String dir, int gen) {
        this.gen = gen;

        this.queue = new LinkedBlockingQueue<>(1000);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOG.debug("shutdown hook cleaning up");
                    WriteAheadLog.this.stop();
                    writerThread.interrupt();
                    os.close();
                    LOG.debug("shutdown hook cleaning up... done, records written: {}, flushes: {}", KeyValueEntry.getRecordsWritten(), KeyValueEntry.getFlushCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        FileOutputStream fos;
        try {
            this.dir = dir;
            fos = new FileOutputStream(walFileName(), true);
            this.os = new DataOutputStream(new BufferedOutputStream(fos, 1024 * 8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        writerThread = new Thread(() -> {
            LOG.debug("wal writer thread started");
            int count = 0;
            int errors = 0;
            while (true) {
                if (stop.get() && queue.isEmpty()) {
                    break;
                }
                try {
                    Object cmd = queue.take();
                    if (cmd instanceof SetCmd) {
                        SetCmd setCmd = (SetCmd) cmd;
                        LOG.debug("appending SetCmd key: {}", setCmd.key);
                        KeyValueEntry entry = new KeyValueEntry((byte) CmdType.Set.ordinal(), setCmd.key, setCmd.value);
                        entry.writeTo(os);
                    } else {
                        errors += 1;
                        LOG.error("unrecognized command: " + cmd);
                    }
                    if ((count += 1) % 10000 == 0) {
                        LOG.info("processed {}, errors {}, queue-length {}", count, errors, queue.size());
                    }
                } catch (InterruptedException e) {
                    LOG.error("caught exception in queue.take, retrying: " + e);
                } catch (IOException e) {
                    LOG.error("caught IOException, exiting: ", e);
                    break;
                }
            }
            LOG.debug("wal writer thread exiting");
        });
        writerThread.start();
    }

    private String walFileName() {
        return this.dir + File.separatorChar + "wal" + gen;
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
                    LOG.debug("replayed from wal: {}, store-size: ", count, memtable.size());
                }
            }
            LOG.debug("replayed from wal: {} keys, store-size: {}, in {} ms", count, memtable.size(), (System.currentTimeMillis() - start));
            is.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(SetCmd cmd) {
        LOG.debug("append to queue {}", cmd.key);
        boolean done = false;
        int attempt = 0;
        do {
            if (stop.get()) {
                throw new IllegalStateException("wal stopped, append not allowed");
            }
            try {
                attempt += 1;
                done = queue.offer(cmd, 10, TimeUnit.MILLISECONDS);
                //if (!done) {
                //System.out.println("retrying append to queue, attempts: " + attempt);
                //}
            } catch (InterruptedException e) {
                LOG.error("retrying append to queue for {}, attempts: {}, exception: {}", cmd.key, attempt, e);
            }
        } while (!done);

        LOG.debug("append to queue done... {}", cmd.key);
        count.incrementAndGet();
    }
}
