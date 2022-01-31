package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class Level {
    public static final Logger LOG = LoggerFactory.getLogger(Level.class);

    private final File dir;
    private final int num;
    private final LinkedList<Segment> segments;

    public Level(String dirName, int num) {
        LOG.info("loading level: {}", num);
        this.num = num;
        this.dir = new File(levelDirName(dirName, num));
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("couldn't create level dir: " + num);
            }
        }

        this.segments = loadSegments(dir);
    }

    public static LinkedList<Segment> loadSegments(File dir) {
        final Comparator<Object> comparator = Comparator.comparingInt(value -> (Integer) value).reversed();
        return Arrays.stream(Objects.requireNonNull(dir.listFiles(pathname -> pathname.getName().startsWith("seg"))))
                .map(file -> Integer.parseInt(file.getName().replace("seg", "")))
                .sorted(comparator)
                .map(n -> new Segment(dir, n))
                .collect(Collectors.toCollection(LinkedList::new));
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
        for (Segment segment : segments) {
            final Optional<String> value = segment.get(key);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    public void addSegment(TreeMap<String, String> memtable) {
        int segmentNumber = nextSegmentNumber();
        segments.addFirst(Segment.create(dir, memtable, segmentNumber));
    }

    private int nextSegmentNumber() {
        if (segments.isEmpty()) {
            return 0;
        }
        return segments.getFirst().getNum() + 1;
    }

    public String dirPathName() {
        return dir.getPath();
    }

    public long keyCount() {
        return segments.stream().mapToLong(Segment::keyCount).sum();
    }
}
