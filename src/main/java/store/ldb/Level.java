package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static store.ldb.StringUtils.isGreaterThan;

public class Level {
    public static final Logger LOG = LoggerFactory.getLogger(Level.class);

    private final File dir;
    private final int num;
    private final Config config;
    private final ConcurrentSkipListMap<Object, Segment> segments;
    private final AtomicInteger nextSegmentNumber;
    private final LevelType levelType;
    private String maxCompactedKey;

    public Level(String dirName, int num, Config config, Manifest manifest) {
        LOG.info("create level: {}", num);
        this.config = config;
        this.num = num;
        this.dir = initDir(dirName, num);
        this.levelType = LevelType.of(num);
        this.segments = new ConcurrentSkipListMap<>(levelType.comparator());
        Segment.loadAll(dir, config, manifest).forEach(this::addSegment);
        nextSegmentNumber = new AtomicInteger(initNextSegmentNumber(segments.values()));
        segments.values().forEach(segment -> LOG.info("level {} segment {}", num, segment));
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
        if (segment.isReady()) {
            throw new IllegalStateException("segment already ready: " + segment + " for level: " + this);
        }
        segment.markReady();
        if (segments.containsKey(levelType.key(segment))) {
            throw new IllegalStateException("segment with key already exists!: " + segment + " in " + segments);
        }
        final Segment prev = segments.put(levelType.key(segment), segment);
        if (prev != null) {
            throw new IllegalStateException(format("segment %s already added to level %s\n", segment.fileName, dirPathName()));
        }
        levelType.assertSegmentInvariants(segments);
    }

    private static String levelDirName(String dir, int num) {
        return dir + File.separatorChar + "level" + num;
    }

    public Optional<String> get(String key, ByteBuffer keyBuf) {
        return levelType.getValue(key, keyBuf, segments);
    }

    public List<Segment> flushMemtable(Memtable memtable) {
        assertIsLevelZero();
        List<Segment> segmentsCreated = new ArrayList<>();
        for (List<Map.Entry<String, String>> partition : memtable.partitions(config.memtablePartitions)) {
            if (partition.isEmpty()) continue;
            Segment segment = createNextSegment();
            segment.writeMemtable(partition);
            addSegment(segment);
            segmentsCreated.add(segment);
        }
        return segmentsCreated;
    }

    private void assertIsLevelZero() {
        if (levelType != LevelType.LEVEL_0) throw new AssertionError("operation allowed only in level0");
    }

    private int nextSegmentNumber() {
        return nextSegmentNumber.getAndIncrement();
    }

    public String dirPathName() {
        return dir.getPath();
    }

    public long keyCount() {
        return segments.values().stream().mapToLong(Segment::keyCount).sum();
    }

    public long totalBytes() {
        return segments.values().stream().mapToLong(Segment::totalBytes).sum();
    }

    public int getNum() {
        return num;
    }

    public void removeSegment(Segment segment) {
        Segment prev = segments.remove(levelType.key(segment));
        if (prev == null)
            throw new IllegalStateException("could not remove segment, was not present in level: " + segment);
        segment.delete();
    }

    @Override
    public String toString() {
        return dirPathName();
    }

    List<Segment> getSegmentsForCompaction() {
        List<Segment> list = segments.values().stream()
                .filter(segment -> !segment.isMarkedForCompaction())
                .collect(Collectors.toCollection(LinkedList::new));

        Integer threshold = config.levelCompactionThreshold.apply(this);
        if (list.size() < threshold) return List.of();

        if (this.getNum() == 0) {
            // for level0 pick the oldest and add all overlapping ones for compaction
            Collections.reverse(list);
            Segment oldest = list.remove(0);
            List<Segment> toCompact = new ArrayList<>();
            toCompact.add(oldest);
            List<Segment> overlappingSegments = getOverlappingSegments(list, oldest.getMinKey(), oldest.getMaxKey());
            //overlappingSegments = overlappingSegments.subList(0, Math.min(overlappingSegments.size(), 10));
            toCompact.addAll(overlappingSegments);
            list = toCompact;
        } else {
            Segment nextSegment = getNextSegmentToCompact(list);
            if (nextSegment == null) return List.of();
            list = List.of(nextSegment);
        }
        return list;
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

    public List<Segment> getOverlappingSegments(Collection<Segment> list, String minKey, String maxKey) {
        if (list == null) {
            assertLevelIsKeySorted();
            list = new ArrayList<>(this.segments.values());
        }
        return list.stream()
                .filter(segment -> segment.overlaps(minKey, maxKey))
                .collect(Collectors.toList());
    }

    public SegmentSpan segmentSpan(String minKey, String maxKey) {
        assertLevelIsKeySorted();
        int count = 0;
        long size = 0;
        for (Segment segment : this.segments.values()) {
            if (!segment.isMarkedForCompaction() && segment.overlaps(minKey, maxKey)) {
                count++;
                size += segment.totalBytes();
            }
        }
        return new SegmentSpan(count, size, minKey, maxKey);
    }

    private void assertLevelIsKeySorted() {
        if (!isKeySorted()) {
            throw new IllegalStateException("can't get overlapping segments of a level this is not key sorted");
        }
    }

    private boolean isKeySorted() {
        return levelType == LevelType.LEVEL_N;
    }

    public int segmentCount() {
        return segments.size();
    }

    public double getCompactionScore() {
        // not using streams api because its showing up as a bottleneck in the profiler
        final List<Segment> segmentsNotBeingCompacted = new ArrayList<>();
        for (Segment seg : segments.values()) {
            if (!seg.isMarkedForCompaction()) {
                segmentsNotBeingCompacted.add(seg);
            }
        }
        return levelType.getCompactionScore(segmentsNotBeingCompacted, config, this);
    }

    public void addStats(Map<String, Object> stats) {
        stats.put(dirPathName() + ".segments", segmentCount());
        stats.put(dirPathName() + ".keyCount", keyCount());
        stats.put(dirPathName() + ".totalBytes", totalBytes());
        segments.values().forEach(segment -> stats.put(segment.fileName, segment));
    }

    record SegmentSpan(int count, long size, String minKey, String maxKey) {
    }

}

