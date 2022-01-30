package store.ldb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class KeyValueEntry {
    public static final int FLUSH_INTERVAL = 0;
    final byte metadata;
    final String key;
    final String value;
    private static long lastFlushTime;
    private static final AtomicLong recordsWritten = new AtomicLong();
    private static final AtomicLong flushCount = new AtomicLong();

    public static KeyValueEntry readFrom(DataInputStream is) throws IOException {
        byte metadata = is.readByte();

        CompressionType keyCmpr = CompressionType.values()[is.readByte()];
        short keyLen = is.readShort();
        String key = new String(is.readNBytes(keyLen));

        CompressionType valCmpr = CompressionType.values()[is.readByte()];
        short valLen = is.readShort();
        String val = new String(is.readNBytes(valLen));

        return new KeyValueEntry(metadata, key, val);
    }

    public int valueOffset() {
        final int metadataLen = 1;
        final int keyComprLen = 1;
        final int keyLen = 2;
        final int valueComprLen = 1;
        final int valueLen = 2;
        return metadataLen + keyComprLen + keyLen + key.length() + valueComprLen + valueLen;
    }

    public int totalLength() {
        return valueOffset() + value.length();
    }

    private enum CompressionType {
        None
    }

    public KeyValueEntry(byte metadata, String key, String value) {
        this.metadata = metadata;
        this.key = key;
        this.value = value;
    }

    public void writeTo(DataOutputStream os) throws IOException {
        os.writeByte(metadata);

        os.writeByte(CompressionType.None.ordinal());
        os.writeShort(key.length());
        os.write(key.getBytes());

        os.writeByte(CompressionType.None.ordinal());
        os.writeShort(value.length());
        os.write(value.getBytes());

        recordsWritten.incrementAndGet();
        if (System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL) { // even keeping this as zero reduces the number of flushes by a factor of 1000
            lastFlushTime = System.currentTimeMillis();
            flushCount.incrementAndGet();
            os.flush();
        }
        //fos.getFD().sync();
    }

    public static long getRecordsWritten() {
        return recordsWritten.get();
    }

    public static long getFlushCount() {
        return flushCount.get();
    }
}
