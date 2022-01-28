package app;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.Attributes;

public class WriteAheadLog {
    private final String dir;
    private final DataOutputStream os;
    private final LinkedBlockingQueue<Object> queue;
    private final Thread writerThread;
    private final AtomicBoolean exiting = new AtomicBoolean(false);

    private enum CmdType {
        Set,
    }

    private enum CompressionType {
        None
    }

    public WriteAheadLog(String dir) {
        this.queue = new LinkedBlockingQueue<>(1000);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("shutdown hook cleaning up");
                    exiting.set(true);
                    writerThread.interrupt();
                    os.flush();
                    os.close();
                    System.out.println("shutdown hook cleaning up... done");
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
                if (exiting.get() && queue.isEmpty()) {
                    break;
                }
                try {
                    Object cmd = queue.take();
                    if (cmd instanceof SetCmd) {
                        writeTo((SetCmd) cmd, os, fos);
                    } else {
                        errors += 1;
                        System.out.println("unrecognized command: " + cmd);
                    }
                    if ((count += 1) % 10000 == 0) {
                        System.out.format("processed %d, errors %d, queue-length %d\n", count, errors, queue.size());
                    }
                } catch (InterruptedException e) {
                    System.out.println("caught exception in queue.take(), retrying: " + e);
                }
            }
            System.out.println("wal writer thread exiting");
        });
        writerThread.start();
    }

    private String walFileName() {
        return this.dir + File.separatorChar + "wal";
    }

    void replay(Store store) {
        File file = new File(walFileName());
        if (!file.exists() || file.length() == 0) {
            return;
        }
        int count = 0;
        try {
            DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(walFileName())));
            while (is.available() > 0) {
                CmdType type = CmdType.values()[is.readByte()];

                CompressionType keyCmpr = CompressionType.values()[is.readByte()];
                short keyLen = is.readShort();
                String key = new String(is.readNBytes(keyLen));

                CompressionType valCmpr = CompressionType.values()[is.readByte()];
                short valLen = is.readShort();
                String val = new String(is.readNBytes(valLen));

                store.setRaw(key, val);

                count += 1;
                if (count % 100000 == 0) {
                    System.out.println("replayed: " + count + " store-size: " + store.memtable.size());
                }
            }
            System.out.println("replayed: " + count + " store-size: " + store.memtable.size());
            is.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(SetCmd cmd) {
        boolean done = false;
        int attempt = 0;
        do {
            if (exiting.get()) {
                throw new IllegalStateException("exiting");
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
    }

    private void writeTo(SetCmd cmd, DataOutputStream os, FileOutputStream fos) {
        try {
            os.writeByte(CmdType.Set.ordinal());

            os.writeByte(CompressionType.None.ordinal());
            os.writeShort(cmd.key.length());
            os.write(cmd.key.getBytes());

            os.writeByte(CompressionType.None.ordinal());
            os.writeShort(cmd.value.length());
            os.write(cmd.value.getBytes());

            os.flush();
            //fos.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
