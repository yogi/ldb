package store.ldb;

import com.google.common.cache.*;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.abbreviate;
import static store.ldb.StringUtils.isGreaterThan;
import static store.ldb.StringUtils.isLessThan;

public class Segment {
    public static final Logger LOG = LoggerFactory.getLogger(Segment.class);
    public static final int LDB_MARKER = 1279541793;

    private static LoadingCache<Segment, ByteBuffer> dataCache;

    final int num;
    final String fileName;
    private final Config config;
    private ConcurrentSkipListMap<String, Block> blocks;
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean markedForCompaction = new AtomicBoolean();
    private SegmentWriter writer;
    private SegmentMetadata metadata;

    public Segment(File dir, int num, Config config) {
        this.num = num;
        this.config = config;
        this.fileName = dir.getPath() + File.separatorChar + "seg" + num;
        LOG.debug("new segment: {}", fileName);
    }

    public static List<Segment> loadAll(File dir, Config config, Manifest manifest) {
        List<Segment> segments = new ArrayList<>();
        for (File file : Objects.requireNonNull(dir.listFiles(pathname -> pathname.getName().startsWith("seg")))) {
            Integer n = Integer.parseInt(file.getName().replace("seg", ""));
            Segment segment = loadSegment(dir, n, config);
            if (segment != null) {
                if (manifest.contains(segment)) {
                    segments.add(segment);
                } else {
                    LOG.info("deleting segment not in manifest: " + segment.fileName);
                    segment.delete();
                }
            }
        }
        return segments;
    }

    private static Segment loadSegment(File dir, Integer n, Config config) {
        Segment segment = new Segment(dir, n, config);
        try {
            segment.load();
            return segment;
        } catch (IOException e) {
            LOG.info("deleting invalid segment {} - caught exception: {}", segment, e.getMessage());
            segment.delete();
            return null;
        }
    }

    public static void resetCache(int segmentCacheSize) {
        dataCache = CacheBuilder
                .newBuilder()
                .maximumWeight(segmentCacheSize)
                .weigher((Weigher<Segment, ByteBuffer>) (segment, buf) -> segment.metadata.blockDataLength())
                .recordStats()
                .build(CacheLoader.from(segment -> {
                    try (FileChannel readChannel = new FileInputStream(segment.fileName).getChannel()) {
                        return readChannel.map(FileChannel.MapMode.READ_ONLY,
                                0,                   // position
                                readChannel.size());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    public static CacheStats cacheStats() {
        return dataCache.stats();
    }

    public void writeMemtable(List<Map.Entry<String, String>> memtable) {
        assertNotReady();
        LOG.debug("write memtable to segment {}", fileName);
        for (Map.Entry<String, String> entry : memtable) {
            getWriter().write(new KeyValueEntry((byte) 0, entry.getKey(), entry.getValue()));
        }
        getWriter().done();
        LOG.debug("done: write memtable to segment {}, {} keys, {} bytes in {} ms", fileName, metadata.keyCount(), metadata.totalBytes, getWriter().timeTaken());
    }

    public SegmentWriter getWriter() {
        if (writer == null) writer = new SegmentWriter();
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

    public KeyValueEntryIterator keyValueEntryIterator() {
        return new KeyValueEntryIterator(blocks.values());
    }

    boolean overlaps(String minKey, String maxKey) {
        return !(isLessThan(getMaxKey(), minKey)
                || isGreaterThan(getMinKey(), maxKey));
    }

    public void copyFrom(Segment segment) {
        try {
            FileUtils.copyFile(new File(segment.fileName), new File(this.fileName));
            load();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean belongsTo(Level level) {
        return fileName.startsWith(level.dirPathName());
    }

    class SegmentWriter {
        private final DataOutputStream os;
        private int offset = 0;
        private long startTime;
        private long endTime;
        private BlockWriter blockWriter;
        private String minKey;
        private String maxKey;
        int keyCount;

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
                blocks = new ConcurrentSkipListMap<>();
                startTime = System.currentTimeMillis();
                minKey = entry.getKey();
                keyCount = 0;
            }

            if (blockWriter == null) {
                blockWriter = new BlockWriter(config, Segment.this);
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
            Block block = blockWriter.writeTo(os, offset);
            offset += block.length;
            blocks.put(block.startKey, block);
            blockWriter = null;
        }

        public void done() {
            assertNotReady();
            try {
                if (blockWriter != null) {
                    flushBlockWriter();
                }

                int blockIndexOffset = offset;
                offset += Block.writeIndex(os, blocks.values());

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
        blocks = Block.loadBlocks(metadata.blockIndexOffset, metadata.offset, fileName, this);
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

    public Optional<String> get(String key, ByteBuffer keyBuf) {
        assertReady();
        final Map.Entry<String, Block> blockEntry = blocks.floorEntry(key);
        if (blockEntry == null) return Optional.empty();
        Block block = blockEntry.getValue();
        Optional<String> value = block.get(key, keyBuf);
        if (value.isPresent()) {
            LOG.debug("get() found key {} in {}", key, this);
            return value;
        }
        return Optional.empty(); // can happen only at Level0
    }

    public ByteBuffer getData() {
        try {
            return dataCache.get(this).duplicate();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNum() {
        return num;
    }

    public int keyCount() {
        return metadata.keyCount;
    }

    public int totalBytes() {
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
                format("[Segment %s min:%s max:%s, markedForCompaction: %s, keys:%d, blocks: %d, size:%.2fKB]",
                        fileName, abbreviate(getMinKey(), 15), abbreviate(getMaxKey(), 15), isMarkedForCompaction(), keyCount(), blocks.size(), totalBytes() / 1024.0);
    }

}
