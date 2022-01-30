package store.ldb;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Level {
    private final File dir;
    private final int num;
    private final List<Segment> segments;

    public Level(String dirName, int num) {
        this.num = num;

        this.dir = new File(levelDirName(dirName, num));
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new RuntimeException("couldn't create level dir: " + num);
            }
        }

        this.segments = Segment.loadSegments(dir);
    }

    private String levelDirName(String dir, int num) {
        return dir + File.separatorChar + "level" + num;
    }

    static TreeMap<Integer, Level> loadLevels(String dir) {
        TreeMap<Integer, Level> levels = new TreeMap<>();
        for (int i = 0; i < 4; i++) {
            Level level = new Level(dir, i);
            levels.put(i, level);
        }
        return levels;
    }

    public Optional<String> get(String key) {
        for (Segment segment : segmentsNewestToOldest()) {
            final Optional<String> value = segment.get(key);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    private List<Segment> segmentsNewestToOldest() {
        return segments.stream()
                .sorted(Comparator.comparing(Segment::getNum).reversed())
                .collect(Collectors.toList());
    }

    public void addSegment(TreeMap<String, String> memtable) {
        int segmentNumber = nextSegmentNumber();
        segments.add(Segment.create(dir, memtable, segmentNumber));
    }

    private int nextSegmentNumber() {
        if (segments.isEmpty()) {
            return 0;
        }
        return segments.get(segments.size() - 1).getNum() + 1;
    }

    public String dirPathName() {
        return dir.getPath();
    }

    public long keyCount() {
        return segments.stream().mapToLong(Segment::keyCount).sum();
    }
}
