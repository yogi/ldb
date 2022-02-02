package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Segment {
    public static final Logger LOG = LoggerFactory.getLogger(Segment.class);

    private final File dir;
    final int num;
    final String fileName;
    private final TreeMap<String, ValuePosition> index = new TreeMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final SegmentWriter writer;
    private int totalBytes;

    public Segment(File dir, int num) {
        this.dir = dir;
        this.num = num;
        this.fileName = dir.getPath() + File.separatorChar + "seg" + num;
        this.writer = new SegmentWriter();
        LOG.info("new segment: {}", fileName);
    }

    public static List<Segment> loadAll(File dir) {
        return Arrays.stream(Objects.requireNonNull(dir.listFiles(pathname -> pathname.getName().startsWith("seg"))))
                .map(file -> Integer.parseInt(file.getName().replace("seg", "")))
                .map(n -> {
                    final Segment segment = new Segment(dir, n);
                    segment.loadIndex();
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
        LOG.info("write memtable to segment {} done: {} keys in {} ms", fileName, index.size(), writer.timeTaken());
    }

    public SegmentWriter getWriter() {
        return writer;
    }

    public String getMaxKey() {
        return index.lastEntry().getKey();
    }

    public String getMinKey() {
        return index.firstEntry().getKey();
    }

    void markReady() {
        ready.set(true);
    }

    class SegmentWriter {
        private final DataOutputStream os;
        private int count = 0;
        private long offset = 0;
        private long startTime;
        private long endTime;

        public SegmentWriter() {
            assertNotReady();
            try {
                this.os = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(fileName, true), 1024 * 8));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public void write(KeyValueEntry entry) {
            if (startTime == 0) {
                index.clear();
                startTime = System.currentTimeMillis();
            }
            try {
                entry.writeTo(os);
                index.put(entry.key, new ValuePosition(offset + entry.valueOffset(), entry.value.length()));
                offset += entry.totalLength();
                count += 1;
                LOG.debug("wrote entry {} to segment {}", entry.key, fileName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void done() {
            if (ready.get()) {
                return;
            }
            try {
                os.close();
                endTime = System.currentTimeMillis();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long writtenBytes() {
            return offset;
        }

        public long timeTaken() {
            return endTime - startTime;
        }
    }

    public void loadIndex() {
        assertNotReady();

        File file = new File(fileName);
        if (!file.exists()) {
            throw new IllegalArgumentException("segment file does not exist: " + file.getPath());
        }

        try {
            int count = 0;
            int offset = 0;
            long start = System.currentTimeMillis();
            DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
            while (is.available() > 0) {
                KeyValueEntry entry = KeyValueEntry.readFrom(is);
                index.put(entry.key, new ValuePosition(offset + entry.valueOffset(), entry.value.length()));
                offset += entry.totalLength();
                count += 1;
                if (count % 100000 == 0) {
                    LOG.debug("loaded from segment: {} store-size: {}", count, index.size());
                }
            }
            totalBytes = offset;
            LOG.debug("loaded from segment: {} keys, store-size: {}, in {} ms", count, index.size(), (System.currentTimeMillis() - start));
            is.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        final ValuePosition pos = index.get(key);
        if (pos == null) {
            return Optional.empty();
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r")) {
            randomAccessFile.seek(pos.offset);
            final byte[] bytes = new byte[pos.len];
            randomAccessFile.read(bytes);
            LOG.debug("found key {} in segment {}/seg{}", key, dir.getPath(), num);
            return Optional.of(new String(bytes));
        } catch (IOException e) {
            throw new RuntimeException("could not get key: " + key + ", exception: " + e);
        }
    }

    public int getNum() {
        return num;
    }

    public long keyCount() {
        return index.size();
    }

    public int totalBytes() {
        return totalBytes;
    }

    public void delete() {
        assertReady();
        File file = new File(fileName);
        LOG.info("delete segment file: " + file.getPath());
        if (file.exists()) {
            if (!file.delete()) {
                final String msg = "could not delete segment file: " + file.getPath();
                LOG.error(msg);
                throw new IllegalStateException(msg);
            }
        }
    }

    public boolean isReady() {
        return ready.get();
    }

    private static class ValuePosition {
        final long offset;

        final int len;

        public ValuePosition(long offset, int len) {
            this.offset = offset;
            this.len = len;
        }

    }

    @Override
    public String toString() {
        return "Segment{" +
                "dir=" + dir +
                ", num=" + num +
                '}';
    }

}
