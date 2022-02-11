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
import static store.ldb.StringUtils.*;
import static store.ldb.Utils.roundTo;

public class Level {
    public static final Logger LOG = LoggerFactory.getLogger(Level.class);
    public static final Comparator<Segment> NUM_DESC_SEGMENT_COMPARATOR = (o1, o2) -> o2.num - o1.num;
    public static final Comparator<Segment> KEY_ASC_SEGMENT_COMPARATOR =
            Comparator.comparing(Segment::getMinKey)
                    .thenComparing(Segment::getMaxKey)
                    .thenComparing(Segment::getNum);

    private final File dir;
    private final int num;
    private final Config config;
    final TreeSet<Segment> segments;
    private final AtomicInteger nextSegmentNumber;
    private final Comparator<Segment> segmentComparator;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private String maxCompactedKey;

    public Level(String dirName, int num, Comparator<Segment> segmentComparator, Config config) {
        this.config = config;
        LOG.info("create level: {}", num);
        this.num = num;
        this.dir = initDir(dirName, num);
        this.segmentComparator = segmentComparator;
        this.segments = new TreeSet<>(segmentComparator);
        Segment.loadAll(dir, config).forEach(this::addSegment);
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
        return new Segment(dir, nextSegmentNumber(), config);
    }

    public void addSegment(Segment segment) {
        lock.writeLock().lock();
        try {
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

    static TreeMap<Integer, Level> loadLevels(String dir, Config config) {
        TreeMap<Integer, Level> levels = new TreeMap<>();
        for (int i = 0; i < config.numLevels; i++) {
            final Comparator<Segment> segmentComparator = i == 0 ? NUM_DESC_SEGMENT_COMPARATOR : KEY_ASC_SEGMENT_COMPARATOR;
            Level level = new Level(dir, i, segmentComparator, config);
            levels.put(i, level);
        }
        return levels;
    }

    public Optional<String> get(String key) {
        lock.readLock().lock();
        try {
            for (Segment segment : segments) {
                if (!segment.isKeyInRange(key)) continue;
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

    @Override
    public String toString() {
        return dirPathName();
    }

    List<Segment> markSegmentsForCompaction() {
        lock.readLock().lock();
        try {
            List<Segment> list = segments.stream().filter(segment -> !segment.isMarkedForCompaction()).collect(Collectors.toCollection(LinkedList::new));
            final Integer threshold = config.levelCompactionThreshold.apply(this);
            if (list.size() < threshold) return List.of();
            if (this.getNum() == 0) {
                // for level0 pick the oldest and add all overlapping ones for compaction
                Collections.reverse(list);
                final Segment picked = list.remove(0);
                List<Segment> toCompact = new ArrayList<>();
                toCompact.add(picked);
                final List<Segment> overlappingSegments = getOverlappingSegments(list, picked.getMinKey(), picked.getMaxKey());
                toCompact.addAll(overlappingSegments.subList(0, Math.min(overlappingSegments.size(), 10)));
                list = toCompact;
                LOG.debug("adding {}/{} overlapping segments to level0 for {}", toCompact.size() - 1, overlappingSegments.size(), picked);
            } else {
                Segment nextSegment = getNextSegmentToCompact(list);
                if (nextSegment == null) return List.of();
                list = List.of(nextSegment);
            }
            list.forEach(Segment::markForCompaction);
            return list;
        } finally {
            lock.readLock().unlock();
        }
    }

    private Segment getNextSegmentToCompact(List<Segment> nonEmptyListOfSegments) {
        Segment nextSegment;
        if (maxCompactedKey == null) {
            nextSegment = nonEmptyListOfSegments.get(0);
        } else {
            final Optional<Segment> optionalNext = nonEmptyListOfSegments.stream().
                    filter(segment -> isGreaterThan(segment.getMinKey(), maxCompactedKey)).
                    findFirst();
            if (optionalNext.isEmpty()) {
                maxCompactedKey = null; // wrap around
                return null;
            }
            nextSegment = optionalNext.get();
        }
        maxCompactedKey = nextSegment.getMaxKey();
        return nextSegment;
    }

    public List<Segment> markOverlappingSegmentsForCompaction(String minKey, String maxKey) {
        assertLevelIsKeySorted();
        lock.readLock().lock();
        try {
            final List<Segment> list = getOverlappingSegments(new ArrayList<>(this.segments), minKey, maxKey);
            if (list.stream().anyMatch(Segment::isMarkedForCompaction)) return List.of();
            list.forEach(Segment::markForCompaction);
            return list;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int segmentsSpannedBy(String minKey, String maxKey) {
        assertLevelIsKeySorted();
        lock.readLock().lock();
        try {
            int count = 0;
            for (Segment segment : this.segments) {
                if (segment.overlaps(minKey, maxKey) && !segment.isMarkedForCompaction()) {
                    count++;
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<Segment> getOverlappingSegments(Collection<Segment> list, String minKey, String maxKey) {
        return list.stream()
                .filter(segment -> segment.overlaps(minKey, maxKey))
                .collect(Collectors.toList());
    }

    private void assertLevelIsKeySorted() {
        if (!isKeySorted()) {
            throw new IllegalStateException("can't get overlapping segments of a level thsi is not key sorted");
        }
    }

    private boolean isKeySorted() {
        return KEY_ASC_SEGMENT_COMPARATOR == segmentComparator;
    }

    public int segmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private double maxBytes() {
        assertLevelIsKeySorted(); // does not apply for level0
        return Math.pow(10, num) * Config.MB;
    }

    public double getCompactionScore() {
        lock.readLock().lock();
        try {
            final List<Segment> notBeingCompacted = segments.stream().filter(segment -> !segment.isMarkedForCompaction()).collect(Collectors.toList());
            if (num == 0) {
                return roundTo(notBeingCompacted.size() / 4.0, 3);
            } else {
                double totalBytes = notBeingCompacted.stream().mapToLong(Segment::totalBytes).sum();
                final double score = totalBytes / maxBytes();
                return roundTo(score, 3);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addStats(Map<String, Object> stats) {
        stats.put(dirPathName() + ".segments", segmentCount());
        stats.put(dirPathName() + ".keyCount", keyCount());
        stats.put(dirPathName() + ".totalBytes", totalBytes());
        segments.forEach(segment -> {
            stats.put(segment.fileName, segment);
        });
    }
}

