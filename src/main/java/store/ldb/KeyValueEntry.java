package store.ldb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class KeyValueEntry {
    public static final int MAX_KEY_SIZE = Short.MAX_VALUE;
    public static final int MAX_VALUE_SIZE = Short.MAX_VALUE;
    public static final int FLUSH_INTERVAL = 0;

    final byte metadata;
    private final String key;
    private byte[] valBytes;
    private String value;
    private static long lastFlushTime;
    private static final AtomicLong recordsWritten = new AtomicLong();
    private static final AtomicLong flushCount = new AtomicLong();

    public KeyValueEntry(byte metadata, String key, byte[] valBytes) {
        this.metadata = metadata;
        this.key = key;
        this.valBytes = valBytes;
    }

    public int valueOffset() {
        final int metadataLen = 1;
        final int keyLen = 2;
        final int valueLen = 2;
        return metadataLen + keyLen + key.length() + valueLen;
    }

    public int totalBytes() {
        return valueOffset() + getValue().length();
    }

    public static KeyValueEntry readFrom(DataInputStream is) throws IOException {
        byte metadata = is.readByte();

        short keyLen = is.readShort();
        String key = new String(is.readNBytes(keyLen));

        short valLen = is.readShort();
        byte[] valBytes = new byte[valLen];
        final int read = is.read(valBytes);
        if (read != valLen) {
            throw new IOException(format("read %d bytes vs expected %d for KeyValueEntry", read, valLen));
        }

        return new KeyValueEntry(metadata, key, valBytes);
    }

    public static KeyValueEntry readFrom(ByteBuffer buf) throws IOException {
        byte metadata = buf.get();

        short keyLen = buf.getShort();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        String key = new String(keyBytes);

        short valLen = buf.getShort();
        byte[] valBytes = new byte[valLen];
        buf.get(valBytes);

        return new KeyValueEntry(metadata, key, valBytes);
    }

    public static Optional<KeyValueEntry> getIfMatches(ByteBuffer buf, ByteBuffer matchKey) throws IOException {
        int originalPos = buf.position();

        byte metadata = buf.get();

        short keyLen = buf.getShort();
        ByteBuffer actualKey = buf.slice(buf.position(), keyLen);

        if (actualKey.equals(matchKey)) {
            buf.position(originalPos);
            return Optional.of(readFrom(buf));
        } else {
            buf.position(buf.position() + keyLen);
            short valLen = buf.getShort();
            buf.position(buf.position() + valLen);
        }
        return Optional.empty();
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

        os.writeShort(valBytes != null ? valBytes.length : value.length());
        os.write(valBytes != null ? valBytes : value.getBytes());

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

    public String getKey() {
        return key;
    }

    public String getValue() {
        if (value == null) value = new String(valBytes);
        return value;
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
