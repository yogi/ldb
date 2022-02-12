package store.ldb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import static java.lang.String.format;

class SegmentMetadata {
    final int offset;
    final int blockIndexOffset;
    final String minKey;
    final String maxKey;
    final int keyCount;
    final int totalBytes;

    public SegmentMetadata(int offset, int blockIndexOffset, String minKey, String maxKey, int keyCount, int totalBytes) {
        this.offset = offset;
        this.blockIndexOffset = blockIndexOffset;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyCount = keyCount;
        this.totalBytes = totalBytes;
    }

    public static SegmentMetadata load(String fileName) throws IOException {
        try (RandomAccessFile f = new RandomAccessFile(fileName, "r")) {
            // seek to the start of fixed length fields
            f.seek(f.length() - (5 * Integer.BYTES));
            int minKeyOffset = f.readInt();
            int blockIndexOffset = f.readInt();
            int keyCount = f.readInt();
            int totalBytes = f.readInt();
            int marker = f.readInt();
            if (marker != Segment.LDB_MARKER) {
                throw new IOException("wrong segment file marker");
            }
            if (f.getFilePointer() != f.length()) {
                throw new IOException("not at EOF");
            }

            // now seek back where min & max keys were written
            f.seek(minKeyOffset);
            short minKeyLen = f.readShort();
            String minKey = readString(f, minKeyLen);
            short maxKeyLen = f.readShort();
            String maxKey = readString(f, maxKeyLen);
            return new SegmentMetadata(minKeyOffset, blockIndexOffset, minKey, maxKey, keyCount, totalBytes);
        }
    }

    public static SegmentMetadata writeTo(int offset, int blockIndexOffset, String minKey, String maxKey, int keyCount, DataOutputStream os) {
        assertMinKeyNotGreaterThanMaxKey(minKey, maxKey);

        try {
            // write the variable length fields first
            os.writeShort(minKey.length());
            os.write(minKey.getBytes());
            os.writeShort(maxKey.length());
            os.write(maxKey.getBytes());

            // now write the fixed length fields
            os.writeInt(offset); // offset is the minKeyOffset where we started writing from
            os.writeInt(blockIndexOffset);
            os.writeInt(keyCount);
            int totalBytes = offset + Short.BYTES + minKey.length() + Short.BYTES + maxKey.length() + (5 * Integer.BYTES);
            os.writeInt(totalBytes);
            os.writeInt(Segment.LDB_MARKER);

            os.flush();

            return new SegmentMetadata(offset, blockIndexOffset, minKey, maxKey, keyCount, totalBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertMinKeyNotGreaterThanMaxKey(String minKey, String maxKey) {
        if (StringUtils.isGreaterThan(minKey, maxKey)) {
            throw new AssertionError(format("minKey greater %s than maxKey %s", minKey, maxKey));
        }
    }

    private static String readString(RandomAccessFile f, short len) throws IOException {
        final byte[] bytes = new byte[len];
        final int read = f.read(bytes);
        if (read != len) {
            throw new RuntimeException(format("expected to read %d bytes, read %d bytes", len, read));
        }
        return new String(bytes);
    }

    public int keyCount() {
        return 0;
    }
}
