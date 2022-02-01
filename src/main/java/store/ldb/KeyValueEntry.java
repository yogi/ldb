package store.ldb;

import org.xerial.snappy.Snappy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class KeyValueEntry {
    public static final int FLUSH_INTERVAL = 0;
    final byte metadata;
    final String key;
    final String value;
    private static long lastFlushTime;
    private static final AtomicLong recordsWritten = new AtomicLong();
    private static final AtomicLong flushCount = new AtomicLong();

    private enum CompressionType {
        NONE((byte) 1) {
            @Override
            public byte[] uncompress(byte[] bytes) {
                return bytes;
            }

            @Override
            public byte[] compress(byte[] bytes) {
                return bytes;
            }
        },
        SNAPPY((byte) 2) {
            @Override
            public byte[] uncompress(byte[] bytes) {
                try {
                    return Snappy.uncompress(bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] compress(byte[] bytes) {
                try {
                    return Snappy.compress(bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        },
        ;
        final byte code;

        CompressionType(byte code) {
            this.code = code;
        }

        public static CompressionType fromCode(byte b) {
            return Arrays.stream(values()).filter(compressionType -> compressionType.code == b).findFirst().get();
        }

        public abstract byte[] uncompress(byte[] bytes);

        public abstract byte[] compress(byte[] bytes);
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

        final CompressionType keyCompression = CompressionType.NONE;
        final byte[] compressedKey = keyCompression.compress(key.getBytes());
        os.writeByte(keyCompression.code);
        os.writeShort(compressedKey.length);
        os.write(compressedKey);

        final CompressionType valueCompression = CompressionType.SNAPPY;
        final byte[] compressedValue = valueCompression.compress(value.getBytes());
        os.writeByte(valueCompression.code);
        os.writeShort(compressedValue.length);
        os.write(compressedValue);

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
