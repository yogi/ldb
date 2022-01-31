package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class Segment {
    public static final Logger LOG = LoggerFactory.getLogger(Segment.class);

    private final File dir;
    private final int num;
    private final Map<String, ValuePosition> index = new TreeMap<>();
    private int totalBytes;

    public static Segment create(File dir, TreeMap<String, String> memtable, int num) {
        writeSegment(dir, memtable, num);
        return new Segment(dir, num);
    }

    public Segment(File dir, int num) {
        LOG.info("loading segment: {} - {}", dir.getPath(), num);
        this.dir = dir;
        this.num = num;
        loadIndex();
    }

    private static void writeSegment(File dir, TreeMap<String, String> memtable, int num) {
        String segFileName = segmentFileName(dir.getPath(), num);
        LOG.debug("write segment: {}", segFileName);

        long start = System.currentTimeMillis();
        try {
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(segFileName, true), 1024 * 8));
            int count = 0;
            for (Map.Entry<String, String> entry : memtable.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                KeyValueEntry keyValueEntry = new KeyValueEntry((byte) 0, k, v);
                keyValueEntry.writeTo(os);
                count += 1;
                LOG.debug("wrote entry to segment {}", k);
            }
            os.close();

            LOG.info("write segment {} done: {} keys in {} ms", segFileName, count, System.currentTimeMillis() - start);
        } catch (IOException e) {
            LOG.error("creating segment... error", e);
            throw new RuntimeException(e);
        }
    }

    private void loadIndex() {
        File file = new File(segmentFileName(dir.getPath(), num));
        if (!file.exists()) {
            throw new IllegalArgumentException("segment file does not exist: " + file.getPath());
        }

        try {
            int count = 0;
            int offset = 0;
            long start = System.currentTimeMillis();
            DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(segmentFileName(dir.getPath(), num))));
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

    private static String segmentFileName(String path, int num) {
        return path + File.separatorChar + "seg" + num;
    }

    public Optional<String> get(String key) {
        final ValuePosition pos = index.get(key);
        if (pos == null) {
            return Optional.empty();
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(segmentFileName(dir.getPath(), num), "r")) {
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
        File file = new File(segmentFileName(dir.getPath(), num));
        LOG.info("delete segment file: " + file.getPath());
        if (file.exists()) {
            if (!file.delete()) {
                final String msg = "could not delete segment file: " + file.getPath();
                LOG.error(msg);
                throw new IllegalStateException(msg);
            }
        }
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
