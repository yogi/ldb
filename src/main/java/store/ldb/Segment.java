package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static store.ldb.StringUtils.isGreaterThanOrEqual;

public class Segment {
    public static final Logger LOG = LoggerFactory.getLogger(Segment.class);
    public static final int KB = 1024;
    public static final int MAX_BLOCK_SIZE = 100 * KB;

    private final File dir;
    final int num;
    final String fileName;
    private List<Block> blocks;
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean markedForCompaction = new AtomicBoolean();
    private final SegmentWriter writer;
    private SegmentMetadata metadata;

    public Segment(File dir, int num) {
        this.dir = dir;
        this.num = num;
        this.fileName = dir.getPath() + File.separatorChar + "seg" + num;
        this.writer = new SegmentWriter();
        LOG.debug("new segment: {}", fileName);
    }

    public static List<Segment> loadAll(File dir) {
        return Arrays.stream(Objects.requireNonNull(dir.listFiles(pathname -> pathname.getName().startsWith("seg"))))
                .map(file -> Integer.parseInt(file.getName().replace("seg", "")))
                .map(n -> {
                    final Segment segment = new Segment(dir, n);
                    segment.load();
                    return segment;
                })
                .collect(Collectors.toList());
    }

    public void writeMemtable(TreeMap<String, String> memtable) {
        assertNotReady();
        LOG.debug("write memtable to segment {}", fileName);
        for (Map.Entry<String, String> entry : memtable.entrySet()) {
            writer.write(new KeyValueEntry((byte) 0, entry.getKey(), entry.getValue()));
        }
        writer.done();
        LOG.debug("done: write memtable to segment {}, {} keys, {} bytes in {} ms", fileName, metadata.keyCount(), metadata.totalBytes, writer.timeTaken());
    }

    public SegmentWriter getWriter() {
        return writer;
    }

    public String getMinKey() {
        return metadata.minKey;
    }

    public String getMaxKey() {
        return metadata.maxKey;
    }

    void markReady() {
        ready.set(true);
    }

    public void markForCompaction() {
        markedForCompaction.set(true);
    }

    public boolean isMarkedForCompaction() {
        return markedForCompaction.get();
    }

    class SegmentWriter {
        private final DataOutputStream os;
        private int offset = 0;
        private long startTime;
        private long endTime;
        private BlockWriter blockWriter;
        private String minKey;
        private String maxKey;
        private int keyCount;

        public SegmentWriter() {
            assertNotReady();
            try {
                this.os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName, true), 1024 * 8));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public void write(KeyValueEntry entry) {
            assertNotReady();

            if (blocks == null) {
                blocks = new ArrayList<>();
                startTime = System.currentTimeMillis();
                minKey = entry.key;
                keyCount = 0;
            }

            if (blockWriter == null) {
                blockWriter = new BlockWriter();
            }

            if (blockWriter.isFull()) {
                flushBlockWriter();
            } else {
                blockWriter.addEntry(entry);
                maxKey = entry.key;
                keyCount += 1;
            }
        }

        private void flushBlockWriter() {
            Block block = blockWriter.writeTo(os, offset, fileName);
            offset += block.length;
            blocks.add(block);
            blockWriter = null;
        }

        public void done() {
            assertNotReady();
            try {
                if (blockWriter != null) {
                    flushBlockWriter();
                }

                int blockIndexOffset = offset;
                offset += Block.writeIndex(os, blocks);

                metadata = SegmentMetadata.writeTo(offset, blockIndexOffset, minKey, maxKey, keyCount, os);
                os.close();
                endTime = System.currentTimeMillis();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean isFull(long limit) {
            assertNotReady();
            return offset >= limit;
        }

        public long timeTaken() {
            return endTime - startTime;
        }
    }

    public void load() {
        assertNotReady();
        metadata = SegmentMetadata.load(fileName);
        blocks = Block.loadBlocks(metadata.blockIndexOffset, fileName);
    }

    private void assertReady() {
        if (!isReady()) {
            throw new IllegalStateException("segment not ready");
        }
    }

    private void assertNotReady() {
        if (isReady()) {
            throw new IllegalStateException("segment is ready");
        }
    }

    public Optional<String> get(String key) {
        assertReady();
        if (!StringUtils.isWithinRange(key, getMinKey(), getMaxKey())) return Optional.empty();
        for (Block block : blocks) {
            if (isGreaterThanOrEqual(key, block.startKey)) {
                Optional<String> value = block.get(key);
                if (value.isPresent()) {
                    return value;
                }
            }
        }
        throw new IllegalStateException(format("should not reach here - key %s not found in blocks for segment %s", key, this));
    }

    public int getNum() {
        return num;
    }

    public long keyCount() {
        return metadata.keyCount;
    }

    public long totalBytes() {
        return metadata.totalBytes;
    }

    public void delete() {
        assertReady();
        File file = new File(fileName);
        LOG.debug("delete segment file: " + file.getPath());
        if (file.exists() && !file.delete()) {
            final String msg = "could not delete segment file: " + file.getPath();
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    public boolean isReady() {
        return ready.get();
    }

    @Override
    public String toString() {
        return metadata == null ?
                format("[Segment %s]", fileName) :
                format("[Segment %s min:%s max:%s]", fileName, getMinKey(), getMaxKey());
    }

    private static class Block {
        final int offset;
        final int length;

        @Override
        public String toString() {
            return "Block{" +
                    "offset=" + offset +
                    ", length=" + length +
                    ", startKey='" + startKey + '\'' +
                    ", compressionType=" + compressionType +
                    '}';
        }

        final String startKey;
        private final CompressionType compressionType;
        private final String filename;

        public Block(String startKey, int offset, int length, CompressionType compressionType, String filename) {
            this.startKey = startKey;
            this.offset = offset;
            this.length = length;
            this.compressionType = compressionType;
            this.filename = filename;
        }

        public static int writeIndex(DataOutputStream os, List<Block> blocks) {
            try {
                int bytesWritten = 0;
                for (Block block : blocks) {
                    bytesWritten += writeIndexEntry(block, os);
                }
                return bytesWritten;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static int writeIndexEntry(Block block, DataOutputStream os) throws IOException {
            os.writeShort(block.startKey.length());
            os.write(block.startKey.getBytes());
            os.writeInt(block.offset);
            os.writeInt(block.length);
            os.writeByte(block.compressionType.code);
            os.flush();
            return Short.BYTES + block.startKey.length() + Integer.BYTES + Integer.BYTES + Byte.BYTES;
        }

        private static Block readIndexEntry(String fileName, DataInputStream is) throws IOException {
            short keyLen = is.readShort();
            String key = new String(is.readNBytes(keyLen));
            int blockOffset = is.readInt();
            int blockLength = is.readInt();
            CompressionType compression = CompressionType.fromCode(is.readByte());
            return new Block(key, blockOffset, blockLength, compression, fileName);
        }

        public static List<Block> loadBlocks(long offset, String fileName) {
            try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)))) {
                final long skippedTo = is.skip(offset);
                if (skippedTo != offset) {
                    throw new IllegalStateException(format("skipped to %d instead of offset %d when loading blocks for segment %s", skippedTo, offset, fileName));
                }
                List<Block> blocks = new ArrayList<>();
                while (is.available() > 0) {
                    blocks.add(readIndexEntry(fileName, is));
                }
                return blocks;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public Optional<String> get(String key) {
            try (DataInputStream is = new DataInputStream(new ByteArrayInputStream(uncompress()))) {
                while (is.available() > 0) {
                    final KeyValueEntry entry = KeyValueEntry.readFrom(is);
                    if (key.equals(entry.key)) {
                        return Optional.of(entry.value);
                    }
                }
                return Optional.empty();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private byte[] uncompress() throws IOException {
            FileInputStream f = new FileInputStream(filename);
            long skippedTo = f.skip(offset);
            if (skippedTo != offset) {
                throw new IllegalStateException(format("skipped to %d instead of offset %d when trying to read block for segment %s", skippedTo, offset, filename));
            }
            return compressionType.uncompress(f.readNBytes(length));
        }
    }

    private static class BlockWriter {
        public static final CompressionType BLOCK_COMPRESSION_TYPE = CompressionType.NONE;
        private final List<KeyValueEntry> entries = new ArrayList<>();
        private int totalBytes;

        public Block writeTo(DataOutputStream os, int offset, String fileName) {
            try {
                byte[] data = compress(entries);
                os.write(data);
                os.flush();
                return new Block(entries.get(0).key, offset, data.length, BLOCK_COMPRESSION_TYPE, fileName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private byte[] compress(List<KeyValueEntry> entries) throws IOException {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(bytes));
            for (KeyValueEntry keyValueEntry : entries) {
                keyValueEntry.writeTo(os);
            }
            return BLOCK_COMPRESSION_TYPE.compress(bytes.toByteArray());
        }

        public boolean isFull() {
            return totalBytes > MAX_BLOCK_SIZE;
        }

        public void addEntry(KeyValueEntry entry) {
            totalBytes += entry.totalBytes();
            entries.add(entry);
        }
    }

    private static class SegmentMetadata {
        final int blockIndexOffset;
        final String minKey;
        final String maxKey;
        final int keyCount;
        final int totalBytes;

        public SegmentMetadata(int blockIndexOffset, String minKey, String maxKey, int keyCount, int totalBytes) {
            this.blockIndexOffset = blockIndexOffset;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.keyCount = keyCount;
            this.totalBytes = totalBytes;
        }

        public static SegmentMetadata load(String fileName) {
            try (RandomAccessFile f = new RandomAccessFile(fileName, "r")) {
                // seek to the start of fixed length fields
                f.seek(f.length() - (4 * Integer.BYTES));
                int blockIndexOffset = f.readInt();
                int minKeyOffset = f.readInt();
                int keyCount = f.readInt();
                int totalBytes = f.readInt();

                // now seek back where min & max keys were written
                f.seek(minKeyOffset);
                short minKeyLen = f.readShort();
                String minKey = readString(f, minKeyLen);
                short maxKeyLen = f.readShort();
                String maxKey = readString(f, maxKeyLen);
                return new SegmentMetadata(blockIndexOffset, minKey, maxKey, keyCount, totalBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static SegmentMetadata writeTo(int offset, int blockIndexOffset, String minKey, String maxKey, int keyCount, DataOutputStream os) {
            try {
                // write the variable lenght fields first
                os.writeShort(minKey.length());
                os.write(minKey.getBytes());
                os.writeShort(maxKey.length());
                os.write(maxKey.getBytes());

                // now write the fixed length fields
                os.writeInt(blockIndexOffset);
                os.writeInt(offset); // offset is the minKeyOffset where we started writing from
                os.writeInt(keyCount);
                int totalBytes = offset + Short.BYTES + minKey.length() + Short.BYTES + maxKey.length() + (4 * Integer.BYTES);
                os.writeInt(totalBytes);

                os.flush();
                return new SegmentMetadata(blockIndexOffset, minKey, maxKey, keyCount, totalBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
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

}
