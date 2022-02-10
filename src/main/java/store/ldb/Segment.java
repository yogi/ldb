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
    public static final int LDB_MARKER = 1279541793;

    final int num;
    final String fileName;
    private final Config config;
    private List<Block> blocks;
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean markedForCompaction = new AtomicBoolean();
    private final SegmentWriter writer;
    private SegmentMetadata metadata;

    public Segment(File dir, int num, Config config) {
        this.num = num;
        this.config = config;
        this.fileName = dir.getPath() + File.separatorChar + "seg" + num;
        this.writer = new SegmentWriter();
        LOG.debug("new segment: {}", fileName);
    }

    public static List<Segment> loadAll(File dir, Config config) {
        return Arrays.stream(Objects.requireNonNull(dir.listFiles(pathname -> pathname.getName().startsWith("seg"))))
                .map(file -> Integer.parseInt(file.getName().replace("seg", "")))
                .map(n -> loadSegment(dir, n, config))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static Segment loadSegment(File dir, Integer n, Config config) {
        Segment segment = new Segment(dir, n, config);
        try {
            segment.load();
            return segment;
        } catch (IOException e) {
            LOG.info("deleting segment {} - caught exception: {}", segment, e.getMessage());
            segment.delete();
            return null;
        }
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

    public Iterator<KeyValueEntry> keyValueEntryIterator() {
        return new KeyValueEntryIterator(blocks);
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

            LOG.trace("write key {} to block-writer for {}", entry.getKey(), Segment.this);

            if (blocks == null) {
                blocks = new ArrayList<>();
                startTime = System.currentTimeMillis();
                minKey = entry.getKey();
                keyCount = 0;
            }

            if (blockWriter == null) {
                blockWriter = new BlockWriter(config);
            }

            blockWriter.addEntry(entry);
            maxKey = entry.getKey();
            keyCount += 1;
            LOG.trace("added key {} to block-writer for {}", entry.getKey(), Segment.this);

            if (blockWriter.isFull(config.maxBlockSize)) {
                flushBlockWriter();
            }
        }

        private void flushBlockWriter() {
            LOG.debug("flushing block for {}", fileName);
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

    public void load() throws IOException {
        assertNotReady();
        metadata = SegmentMetadata.load(fileName);
        blocks = Block.loadBlocks(metadata.blockIndexOffset, metadata.offset, fileName);
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

    public boolean isKeyInRange(String key) {
        return StringUtils.isWithinRange(key, getMinKey(), getMaxKey());
    }

    public Optional<String> get(String key) {
        assertReady();
        for (Block block : blocks) {
            if (isGreaterThanOrEqual(key, block.startKey)) {
                Optional<String> value = block.get(key);
                if (value.isPresent()) {
                    LOG.debug("get() found key {} in {}", key, this);
                    return value;
                }
            }
        }
        return Optional.empty(); // can happen only at Level0
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

}
