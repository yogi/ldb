package store.ldb;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.hasCause;
import static store.ldb.StringUtils.isGreaterThan;
import static store.ldb.Utils.roundTo;
import static store.ldb.Utils.shouldNotGetHere;

public class Level {
    public static final Logger LOG = LoggerFactory.getLogger(Level.class);
    public static final Comparator<Segment> NUM_DESC_SEGMENT_COMPARATOR = Comparator.comparing(Segment::getNum).reversed();
    public static final Comparator<Segment> KEY_ASC_SEGMENT_COMPARATOR =
            Comparator.comparing(Segment::getMinKey)
                    .thenComparing(Segment::getNum);

    private final File dir;
    private final int num;
    private final Config config;
    final ConcurrentSkipListSet<Segment> segments;
    private final AtomicInteger nextSegmentNumber;
    private final Comparator<Segment> segmentComparator;
    private String maxCompactedKey;

    public Level(String dirName, int num, Comparator<Segment> segmentComparator, Config config, Manifest manifest) {
        this.config = config;
        LOG.info("create level: {}", num);
        this.num = num;
        this.dir = initDir(dirName, num);
        this.segmentComparator = segmentComparator;
        this.segments = new ConcurrentSkipListSet<>(segmentComparator);
        Segment.loadAll(dir, config, manifest).forEach(this::addSegment);
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
        if (num > 0) assertSegmentsAreNonOverlapping();
    }

    private void assertSegmentsAreNonOverlapping() {
        final List<Segment> list = new ArrayList<>(new ArrayList<>(segments));
        for (int i = 0; i < list.size() - 1; i++) {
            Segment segment = list.get(i);
            Segment nextSegment = list.get(i + 1);
            if (segment.isMarkedForCompaction() || nextSegment.isMarkedForCompaction()) continue;
            if (isGreaterThan(segment.getMaxKey(), nextSegment.getMinKey())) {
                shouldNotGetHere(format("found overlapping segments %s, %s", segment, nextSegment));
            }
        }
    }

    private static String levelDirName(String dir, int num) {
        return dir + File.separatorChar + "level" + num;
    }

    static TreeMap<Integer, Level> loadLevels(String dir, Config config, Manifest manifest) {
        TreeMap<Integer, Level> levels = new TreeMap<>();
        for (int i = 0; i < config.numLevels; i++) {
            final Comparator<Segment> segmentComparator = i == 0 ? NUM_DESC_SEGMENT_COMPARATOR : KEY_ASC_SEGMENT_COMPARATOR;
            Level level = new Level(dir, i, segmentComparator, config, manifest);
            levels.put(i, level);
        }
        return levels;
    }

    public Optional<String> get(String key, ByteBuffer keyBuf) {
        for (Segment segment : segments) {
            if (!segment.isKeyInRange(key)) continue;
            try {
                Optional<String> value = segment.get(key, keyBuf);
                if (value.isPresent()) {
                    return value;
                }
            } catch (RuntimeException e) {
                if (hasCause(e, FileNotFoundException.class)) {
                    LOG.error("ignoring error in Segment.get(), which is caused by concurrent deletion of segment {} by compaction", segment, e);
                } else {
                    throw e;
                }
            }
        }
        return Optional.empty();
    }

    public List<Segment> flushMemtable(SortedMap<String, String> memtable) {
        final List<Map.Entry<String, String>> original = new ArrayList<>(memtable.entrySet());
        final List<List<Map.Entry<String, String>>> partitions = original.size() < config.memtablePartitions ?
                List.of(original) :
                Lists.partition(original, original.size() / config.memtablePartitions);
        List<Segment> segmentsCreated = new ArrayList<>();
        for (List<Map.Entry<String, String>> partition : partitions) {
            if (partition.isEmpty()) continue;
            Segment segment = createNextSegment();
            segment.writeMemtable(partition);
            addSegment(segment);
            segmentsCreated.add(segment);
        }
        return segmentsCreated;
    }

    private int nextSegmentNumber() {
        return nextSegmentNumber.getAndIncrement();
    }

    public String dirPathName() {
        return dir.getPath();
    }

    public long keyCount() {
        return segments.stream().mapToLong(Segment::keyCount).sum();
    }

    public long totalBytes() {
        return segments.stream().mapToLong(Segment::totalBytes).sum();
    }

    public int getNum() {
        return num;
    }

    public void removeSegment(Segment segment) {
        if (!segments.remove(segment)) {
            throw new IllegalStateException("could not remove segment: " + segment);
        }
        segment.delete();
    }

    @Override
    public String toString() {
        return dirPathName();
    }

    List<Segment> getSegmentsForCompaction() {
        List<Segment> list = segments.stream()
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
            list = new ArrayList<>(this.segments);
        }
        return list.stream()
                .filter(segment -> segment.overlaps(minKey, maxKey))
                .collect(Collectors.toList());
    }

    public SegmentSpan segmentSpan(String minKey, String maxKey) {
        assertLevelIsKeySorted();
        int count = 0;
        long size = 0;
        for (Segment segment : this.segments) {
            if (!segment.isMarkedForCompaction() && segment.overlaps(minKey, maxKey)) {
                count++;
                size += segment.totalBytes();
            }
        }
        return new SegmentSpan(count, size, minKey, maxKey);
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
        return segments.size();
    }

    public double getCompactionScore() {
        // not using streams api because its showing up as a bottleneck in the profiler
        final List<Segment> notBeingCompacted = new ArrayList<>();
        for (Segment seg : segments) {
            if (!seg.isMarkedForCompaction()) {
                notBeingCompacted.add(seg);
            }
        }
        if (num == 0) {
            return roundTo(notBeingCompacted.size() / (double) config.memtablePartitions, 3);
        } else {
            // not using streams api because its showing up as a bottleneck in the profiler
            long totalBytes = 0L;
            for (Segment segment : notBeingCompacted) {
                totalBytes += segment.totalBytes();
            }
            double thresholdBytes = config.levelCompactionThreshold.apply(this) * config.maxSegmentSize;
            final double score = (double) totalBytes / thresholdBytes;
            return roundTo(score, 3);
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

    record SegmentSpan(int count, long size, String minKey, String maxKey) {
    }
}

