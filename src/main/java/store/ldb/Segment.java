package store.ldb;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Segment {
    private final File dir;
    private final int num;
    private final Map<String, ValuePosition> index;

    public static List<Segment> loadSegments(File dir) {
        return Arrays.stream(Objects.requireNonNull(dir.listFiles(pathname -> pathname.getName().startsWith("seg"))))
                .map(file -> Integer.parseInt(file.getName().replace("seg", "")))
                .map(n -> new Segment(dir, n))
                .collect(Collectors.toList());
    }

    public static Segment create(File dir, TreeMap<String, String> memtable, int num) {
        writeSegment(dir, memtable, num);
        return new Segment(dir, num);
    }

    public Segment(File dir, int num) {
        this.dir = dir;
        this.num = num;
        this.index = loadIndex();
    }

    private static void writeSegment(File dir, TreeMap<String, String> memtable, int num) {
        String segFileName = segmentFileName(dir.getPath(), num);
        System.out.println("creating segment: " + segFileName);

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
            }
            os.close();

            System.out.printf("creating segment... done %d keys in %d ms%n", count, System.currentTimeMillis() - start);
        } catch (IOException e) {
            System.out.printf("creating segment... error: %s\n", e);
            throw new RuntimeException(e);
        }
    }

    private Map<String, ValuePosition> loadIndex() {
        File file = new File(segmentFileName(dir.getPath(), num));
        if (!file.exists()) {
            throw new IllegalArgumentException("segment file does not exist: " + file.getPath());
        }

        try {
            Map<String, ValuePosition> map = new TreeMap<>();
            int count = 0;
            int offset = 0;
            long start = System.currentTimeMillis();
            DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(segmentFileName(dir.getPath(), num))));
            while (is.available() > 0) {
                KeyValueEntry entry = KeyValueEntry.readFrom(is);
                map.put(entry.key, new ValuePosition(offset + entry.valueOffset(), entry.value.length()));
                offset += entry.totalLength();
                count += 1;
                if (count % 100000 == 0) {
                    System.out.println("loaded from segment: " + count + " store-size: " + map.size());
                }
            }
            System.out.printf("loaded from segment: %d keys, store-size: %d, in %d ms%n", count, map.size(), (System.currentTimeMillis() - start));
            is.close();

            return map;
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
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(segmentFileName(dir.getPath(), num), "r")) {
            randomAccessFile.seek(pos.offset);
            final byte[] bytes = new byte[pos.len];
            randomAccessFile.read(bytes);
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

    private static class ValuePosition {
        final long offset;
        final int len;

        public ValuePosition(long offset, int len) {
            this.offset = offset;
            this.len = len;
        }
    }
}
