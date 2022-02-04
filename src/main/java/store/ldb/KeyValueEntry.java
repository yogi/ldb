package store.ldb;

import org.xerial.snappy.Snappy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class KeyValueEntry {
    public static final int MAX_KEY_SIZE = Short.MAX_VALUE;
    public static final int MAX_VALUE_SIZE = Short.MAX_VALUE;
    public static final int FLUSH_INTERVAL = 0;
    public static final CompressionType KEY_COMPRESSION = CompressionType.NONE;
    public static final CompressionType VALUE_COMPRESSION = CompressionType.NONE;

    final byte metadata;
    final String key;
    final String value;
    private static long lastFlushTime;
    private static final AtomicLong recordsWritten = new AtomicLong();
    private static final AtomicLong flushCount = new AtomicLong();

    public int valueOffset() {
        final int metadataLen = 1;
        final int keyComprLen = 1;
        final int keyLen = 2;
        final int valueComprLen = 1;
        final int valueLen = 2;
        return metadataLen + keyComprLen + keyLen + key.length() + valueComprLen + valueLen;
    }

    public int totalBytes() {
        return valueOffset() + value.length();
    }

    public static KeyValueEntry readFrom(DataInputStream is) throws IOException {
        byte metadata = is.readByte();

        CompressionType keyCompression = CompressionType.fromCode(is.readByte());
        short keyLen = is.readShort();
        String key = new String(keyCompression.uncompress(is.readNBytes(keyLen)));

        CompressionType valCompression = CompressionType.fromCode(is.readByte());
        short valLen = is.readShort();
        String val = new String(valCompression.uncompress(is.readNBytes(valLen)));

        return new KeyValueEntry(metadata, key, val);
    }

    public KeyValueEntry(byte metadata, String key, String value) {
        this.metadata = metadata;
        this.key = key;
        this.value = value;
    }

    public void writeTo(DataOutputStream os) throws IOException {
        os.writeByte(metadata);

        final byte[] compressedKey = KEY_COMPRESSION.compress(key.getBytes());
        os.writeByte(KEY_COMPRESSION.code);
        os.writeShort(compressedKey.length);
        os.write(compressedKey);

        final byte[] compressedValue = VALUE_COMPRESSION.compress(value.getBytes());
        os.writeByte(VALUE_COMPRESSION.code);
        os.writeShort(compressedValue.length);
        os.write(compressedValue);

        recordsWritten.incrementAndGet();

        // setting FLUSH_INTERVAL to as low as zero can reduce the number of flushes by a factor of 1000 in a high load environment
        if (System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL) {
            lastFlushTime = System.currentTimeMillis();
            flushCount.incrementAndGet();
            os.flush();
//            if (fsync) {
//                fos.getFD().sync();
//            }
        }
    }

    public static long getRecordsWritten() {
        return recordsWritten.get();
    }

    public static long getFlushCount() {
        return flushCount.get();
    }
}
