package store.rdb;

import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteAheadLog {
    final int gen;
    final String dir;
    private final DataOutputStream os;
    private final LinkedBlockingQueue<Object> queue;
    private final Thread writerThread;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicInteger count = new AtomicInteger();

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
                    System.out.println("shutdown hook cleaning up");
                    WriteAheadLog.this.stop();
                    writerThread.interrupt();
                    os.close();
                    System.out.printf("shutdown hook cleaning up... done, records written: %d, flushes: %d\n", KeyValueEntry.getRecordsWritten(), KeyValueEntry.getFlushCount());
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
            System.out.println("wal writer thread started");
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
                        KeyValueEntry entry = new KeyValueEntry((byte) CmdType.Set.ordinal(), setCmd.key, setCmd.value);
                        entry.writeTo(os);
                    } else {
                        errors += 1;
                        System.out.println("unrecognized command: " + cmd);
                    }
                    if ((count += 1) % 10000 == 0) {
                        System.out.format("processed %d, errors %d, queue-length %d\n", count, errors, queue.size());
                    }
                } catch (InterruptedException e) {
                    System.out.println("caught exception in queue.take(), retrying: " + e);
                } catch (IOException e) {
                    System.out.println("caught IOException, exiting: " + e);
                    break;
                }
            }
            System.out.println("wal writer thread exiting");
        });
        writerThread.start();
    }

    private String walFileName() {
        return this.dir + File.separatorChar + "wal" + gen;
    }

    void replay(RDB store) {
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
                store.setRaw(entry.key, entry.value);
                count += 1;
                if (count % 100000 == 0) {
                    System.out.println("replayed from wal: " + count + " store-size: " + store.memtable.size());
                }
            }
            System.out.printf("replayed from wal: %d keys, store-size: %d, in %d ms%n", count, store.memtable.size(), (System.currentTimeMillis() - start));
            is.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(SetCmd cmd) {
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
                System.out.println("retrying append to queue, attempts: " + attempt + " exception: " + e);
            }
        } while (!done);

        count.incrementAndGet();
    }
}
