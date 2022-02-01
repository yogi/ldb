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
    private final LinkedBlockingQueue<Object> queue;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicInteger totalBytes = new AtomicInteger();

    public static WriteAheadLog init(String dir, Level levelZero) {
        int nextGen = 0;
        LinkedList<WriteAheadLog> wals = loadWals(dir);
        if (!wals.isEmpty()) {
            TreeMap<String, String> map = new TreeMap<>();
            wals.forEach(wal -> wal.replay(map));
            if (map.size() > 0) {
                levelZero.flushMemtable(map);
            }
            wals.forEach(WriteAheadLog::delete);
            nextGen = wals.getLast().gen + 1;
        }
        return new WriteAheadLog(dir, nextGen);
    }

    public WriteAheadLog(String dir, int gen) {
        LOG.info("init wal {} {}", dir, gen);

        this.gen = gen;

        this.queue = new LinkedBlockingQueue<>(1000);

        this.dir = dir;
        Thread writerThread = new Thread(() -> {
            LOG.info("wal writer thread started");

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
                if (stop.get() && queue.isEmpty()) {
                    break;
                }
                try {
                    Object cmd = queue.take();
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
                        LOG.info("processed {}, errors {}, queue-length {}", count, errors, queue.size());
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
            LOG.info("wal writer thread exiting");
        });
        writerThread.start();
    }

    public static LinkedList<WriteAheadLog> loadWals(String dir) {
        return Arrays.stream(Objects.requireNonNull(new File(dir)
                        .listFiles((dir1, name) -> name.startsWith("wal"))))
                .map(file -> Integer.parseInt(file.getName().replace("wal", "")))
                .sorted()
                .map(gen -> new WriteAheadLog(dir, gen))
                .collect(Collectors.toCollection(LinkedList::new));
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

    public WriteAheadLog createNext() {
        return new WriteAheadLog(dir, gen + 1);
    }

    private enum CmdType {
        Set(1),
        ;

        final int code;

        CmdType(int code) {
            this.code = code;
        }
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
            } catch (InterruptedException e) {
                LOG.error("retrying append to queue for {}, attempts: {}, exception: {}", cmd.key, attempt, e);
            }
        } while (!done);
        LOG.debug("append to queue done for key {} after {} attempts", cmd.key, attempt);
    }
}
