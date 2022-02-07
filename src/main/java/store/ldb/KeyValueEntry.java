package store.ldb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class KeyValueEntry {
    public static final int MAX_KEY_SIZE = Short.MAX_VALUE;
    public static final int MAX_VALUE_SIZE = Short.MAX_VALUE;
    public static final int FLUSH_INTERVAL = 0;

    final byte metadata;
    final String key;
    final String value;
    private static long lastFlushTime;
    private static final AtomicLong recordsWritten = new AtomicLong();
    private static final AtomicLong flushCount = new AtomicLong();

    public int valueOffset() {
        final int metadataLen = 1;
        final int keyLen = 2;
        final int valueLen = 2;
        return metadataLen + keyLen + key.length() + valueLen;
    }

    public int totalBytes() {
        return valueOffset() + value.length();
    }

    public static KeyValueEntry readFrom(DataInputStream is) throws IOException {
        byte metadata = is.readByte();

        short keyLen = is.readShort();
        String key = new String(is.readNBytes(keyLen));

        short valLen = is.readShort();
        String val = new String(is.readNBytes(valLen));

        return new KeyValueEntry(metadata, key, val);
    }

    public KeyValueEntry(byte metadata, String key, String value) {
        this.metadata = metadata;
        this.key = key;
        this.value = value;
    }

    public void writeTo(DataOutputStream os) throws IOException {
        os.writeByte(metadata);

        os.writeShort(key.length());
        os.write(key.getBytes());

        os.writeShort(value.length());
        os.write(value.getBytes());

        recordsWritten.incrementAndGet();

        // setting FLUSH_INTERVAL to as low as zero can reduce the number of flushes by a factor of 1000 in a high load environment
        if (System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL) {
            lastFlushTime = System.currentTimeMillis();
            flushCount.incrementAndGet();
            os.flush();
            // following can be used to actually write to disk vs only the OS buffer cache
            // if (fsync) {
            //    fos.getFD().sync();
            // }
        }
    }

    public static long getRecordsWritten() {
        return recordsWritten.get();
    }

    public static long getFlushCount() {
        return flushCount.get();
    }

    @Override
    public String toString() {
        return "KeyValueEntry{" +
                "metadata=" + metadata +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
