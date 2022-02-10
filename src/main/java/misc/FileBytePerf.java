package misc;

import java.io.*;
import java.nio.ByteBuffer;

public class FileBytePerf {
    private static final int ITERATIONS = 5;
    private static final double MEG = (Math.pow(1024, 2));
    private static final int RECORD_COUNT = 10_000_000;
    private static final String RECORD = "Help I am trapped in a fortune cookie factory...\n";
    private static final int RECSIZE = RECORD.getBytes().length;

    public static void main(String[] args) throws Exception {
        final int size = RECORD_COUNT * RECSIZE;
        ByteBuffer buf = ByteBuffer.allocate(size);
        for (int i = 0; i < RECORD_COUNT; i++) {
            buf.put(RECORD.getBytes());
        }
        System.out.println(size / MEG + " MB");

        for (int i = 0; i < ITERATIONS; i++) {
            System.out.println("\nIteration " + i);

            final byte[] bytes = buf.array();
            writeRaw(bytes);
            writeBuffered(bytes, 8192);
            writeBuffered(bytes, (int) MEG);
            writeBuffered(bytes, 4 * (int) MEG);
        }
    }

    private static void writeRaw(byte[] bytes) throws IOException {
        File file = File.createTempFile("foo", ".txt");
        try {
            FileOutputStream os = new FileOutputStream(file);
            System.out.print("Writing raw... ");
            write(bytes, os);
        } finally {
            // comment this out if you want to inspect the files afterward
            file.delete();
        }
    }

    private static void writeBuffered(byte[] records, int bufSize) throws IOException {
        File file = File.createTempFile("foo", ".txt");
        try {
            FileOutputStream os = new FileOutputStream(file);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(os, bufSize);

            System.out.print("Writing buffered (buffer size: " + bufSize + ")... ");
            write(records, bufferedOutputStream);
        } finally {
            // comment this out if you want to inspect the files afterward
            file.delete();
        }
    }

    private static void write(byte[] bytes, OutputStream os) throws IOException {
        long start = System.currentTimeMillis();
        os.write(bytes);
        // writer.flush(); // close() should take care of this
        os.close();
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000f + " seconds");

    }
}
