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
    private final int num;
    private final String fileName;
    private final Map<String, ValuePosition> index = new TreeMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private int totalBytes;

    public Segment(File dir, int num) {
        LOG.info("loading segment: {} - {}", dir.getPath(), num);
        this.dir = dir;
        this.num = num;
        this.fileName = dir.getPath() + File.separatorChar + "seg" + num;
    }

    public void writeMemtable(TreeMap<String, String> memtable) {
        assertNotReady();

        LOG.debug("write memtable to segment: {}", fileName);

        long start = System.currentTimeMillis();
        try {
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(fileName, true), 1024 * 8));
            int count = 0;
            long offset = 0;
            for (Map.Entry<String, String> entry : memtable.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                KeyValueEntry keyValueEntry = new KeyValueEntry((byte) 0, k, v);
                keyValueEntry.writeTo(os);
                index.put(keyValueEntry.key, new ValuePosition(offset + keyValueEntry.valueOffset(), keyValueEntry.value.length()));
                offset += keyValueEntry.totalLength();
                count += 1;
                LOG.debug("wrote entry to segment {}", k);
            }
            os.close();

            LOG.info("write segment {} done: {} keys in {} ms", fileName, count, System.currentTimeMillis() - start);

            ready.set(true);
        } catch (IOException e) {
            LOG.error("creating segment... error", e);
            throw new RuntimeException(e);
        }
    }

    public void compactAll(List<Segment> segments) {
        assertNotReady();

        LOG.debug("compacting {} segments to segment {}", segments.size(), fileName);

        try {
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(fileName, true), 1024 * 8));

            int count = 0;
            long offset = 0;
            long start = System.currentTimeMillis();

            PriorityQueue<SegmentScanner> scanners = segments.stream()
                    .map(SegmentScanner::new)
                    .filter(SegmentScanner::hasNext)
                    .collect(Collectors.toCollection(PriorityQueue::new));

            do {
                PriorityQueue<SegmentScanner> nextScanners = new PriorityQueue<>();

                SegmentScanner scanner = scanners.peek();
                if (scanner == null) {
                    break;
                } else {
                    final KeyValueEntry entry = scanner.peek();

                    entry.writeTo(os);
                    index.put(entry.key, new ValuePosition(offset + entry.valueOffset(), entry.value.length()));
                    offset += entry.totalLength();
                    count += 1;

                    while ((scanner = scanners.poll()) != null) {
                        scanner.moveToNextIfEquals(entry.key);
                        if (scanner.hasNext()) {
                            nextScanners.add(scanner);
                        }
                    }

                    scanners = nextScanners;
                }
            } while (true);

            os.close();
            LOG.info("compacting {} segments to segment {} done: {} keys in {} ms", segments.size(), fileName, count, System.currentTimeMillis() - start);

            ready.set(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class SegmentScanner implements Comparable<SegmentScanner> {
        private final Segment segment;
        private final DataInputStream is;
        private KeyValueEntry next;

        public SegmentScanner(Segment segment) {
            try {
                this.segment = segment;
                is = new DataInputStream(new BufferedInputStream(new FileInputStream(segment.fileName)));
                if (is.available() > 0) {
                    next = KeyValueEntry.readFrom(is);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int compareTo(Segment.SegmentScanner other) {
            int result = peek().key.compareTo(other.peek().key);
            if (result != 0) {
                return result;
            }
            return other.segment.num - segment.num;
        }

        private KeyValueEntry peek() {
            return next;
        }

        public boolean hasNext() {
            return next != null;
        }

        public void moveToNextIfEquals(String key) {
            try {
                if (key.equals(next.key)) {
                    if (is.available() > 0) {
                        next = KeyValueEntry.readFrom(is);
                    } else {
                        is.close();
                        next = null;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            return "SegmentScanner{" +
                    "segment=" + segment.num +
                    ", next=" + (next == null ? "null" : next.key) +
                    '}';
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

            ready.set(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertNotReady() {
        if (ready.get()) {
            throw new IllegalStateException("cannot modify an existing segment");
        }
    }

    public Optional<String> get(String key) {
        if (!ready.get()) {
            return Optional.empty();
        }
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

    public void replayTo(Map<String, String> map) {
        index.forEach((key, valuePosition) -> map.put(key, get(key).get()));
    }

    public void delete() {
        if (!ready.get()) {
            throw new IllegalStateException("cannot delete a segment that is not ready: ");
        }
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
