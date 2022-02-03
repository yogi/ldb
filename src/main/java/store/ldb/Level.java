package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Level {
    public static final Logger LOG = LoggerFactory.getLogger(Level.class);
    public static final Comparator<Segment> SEGMENT_NUM_DESC_COMPARATOR = (o1, o2) -> o2.num - o1.num;
    public static final Comparator<Segment> SEGMENT_KEY_ASC_COMPARATOR =
            Comparator.comparing(Segment::getMinKey)
                    .thenComparing(Segment::getMaxKey)
                    .thenComparing(Segment::getNum);

    private final File dir;
    private final int num;
    private final int maxSegmentSize;
    private final TreeSet<Segment> segments;
    private final AtomicInteger nextSegmentNumber;
    private final Comparator<Segment> segmentComparator;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Level(String dirName, int num, int maxSegmentSize, Comparator<Segment> segmentComparator) {
        LOG.info("create level: {}", num);
        this.num = num;
        this.dir = initDir(dirName, num);
        this.maxSegmentSize = maxSegmentSize;
        this.segmentComparator = segmentComparator;
        this.segments = new TreeSet<>(segmentComparator);
        Segment.loadAll(dir).forEach(this::addSegment);
        nextSegmentNumber = new AtomicInteger(initNextSegmentNumber(segments));
        segments.forEach(segment -> LOG.info("level {} segment {}", num, segment));
    }

    private static int initNextSegmentNumber(Collection<Segment> segments) {
        return segments.isEmpty() ?
                0 :
                Collections.max(segments.stream().map(s -> s.num).collect(Collectors.toList())) + 1;
    }

    private static File initDir(String dirName, int num) {
        final File dir = new File(levelDirName(dirName, num));
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("couldn't create level dir: " + num);
            }
        }
        return dir;
    }

    public Segment createNextSegment() {
        return new Segment(dir, nextSegmentNumber());
    }

    public void addSegment(Segment segment) {
        lock.writeLock().lock();
        try {
            LOG.debug("addSegment {} to level {}, segments {}", segment, dirPathName(), segments);
            if (segment.isReady()) {
                throw new IllegalStateException("segment already ready: " + segment + " for level: " + this);
            }
            segment.markReady();
            if (segments.contains(segment)) {
                throw new IllegalStateException("segment already exists!: " + segment + " in " + segments);
            }
            if (!segments.add(segment)) {
                throw new IllegalStateException(format("segment %s was not added to level %s\n", segment.fileName, dirPathName()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static String levelDirName(String dir, int num) {
        return dir + File.separatorChar + "level" + num;
    }

    static TreeMap<Integer, Level> loadLevels(String dir, int maxSegmentSize, int numLevels) {
        TreeMap<Integer, Level> levels = new TreeMap<>();
        for (int i = 0; i < numLevels; i++) {
            final Comparator<Segment> segmentComparator = i == 0 ? SEGMENT_NUM_DESC_COMPARATOR : SEGMENT_KEY_ASC_COMPARATOR;
            Level level = new Level(dir, i, maxSegmentSize, segmentComparator);
            levels.put(i, level);
        }
        return levels;
    }

    public Optional<String> get(String key) {
        lock.readLock().lock();
        try {
            for (Segment segment : segments) {
                Optional<String> value = segment.get(key);
                if (value.isPresent()) {
                    return value;
                }
            }
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void flushMemtable(TreeMap<String, String> memtable) {
        Segment segment = createNextSegment();
        segment.writeMemtable(memtable);
        addSegment(segment);
    }

    private int nextSegmentNumber() {
        return nextSegmentNumber.getAndIncrement();
    }

    public String dirPathName() {
        return dir.getPath();
    }

    public long keyCount() {
        lock.readLock().lock();
        try {
            return segments.stream().mapToLong(Segment::keyCount).sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long totalBytes() {
        lock.readLock().lock();
        try {
            return segments.stream().mapToLong(Segment::totalBytes).sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getNum() {
        return num;
    }

    public void removeSegment(Segment segment) {
        lock.writeLock().lock();
        try {
            if (!segments.remove(segment)) {
                throw new IllegalStateException("could not remove segment: " + segment);
            }
            segment.delete();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long maxSegmentSize() {
        return (long) maxSegmentSize * (this.num + 1);
    }

    @Override
    public String toString() {
        return dirPathName();
    }

    List<Segment> getSegmentsForCompaction() {
        lock.readLock().lock();
        try {
            final List<Segment> list = new ArrayList<>(new ArrayList<>(segments));
            if (segmentComparator == SEGMENT_NUM_DESC_COMPARATOR) Collections.reverse(list);
            return list;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Segment> getOverlappingSegments(String minKey, String maxKey) {
        assertLevelIsKeySorted();

        lock.readLock().lock();
        try {
            if (this.segments.isEmpty()) return Collections.emptyList();

            return new ArrayList<>(this.segments).stream()
                    .filter(segment -> between(segment.getMinKey(), minKey, maxKey)
                            || between(segment.getMaxKey(), minKey, maxKey)
                            || (lessThanOrEqual(segment.getMinKey(), minKey) && greaterThanOrEqual(segment.getMaxKey(), maxKey)))
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean between(String s, String from, String to) {
        return greaterThanOrEqual(s, from) && lessThanOrEqual(s, to);
    }

    private boolean greaterThanOrEqual(String s, String other) {
        return s.compareTo(other) >= 0;
    }

    private boolean lessThanOrEqual(String s, String other) {
        return s.compareTo(other) <= 0;
    }

    private void assertLevelIsKeySorted() {
        if (SEGMENT_KEY_ASC_COMPARATOR != segmentComparator) {
            throw new IllegalStateException("can't get overlapping segments of a level thsi is not key sorted");
        }
    }

    public int segmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}

