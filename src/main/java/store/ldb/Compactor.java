package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);

    private final Config config;
    private final Thread compactorThread;
    private final List<LevelCompactor> levelCompactors = new ArrayList<>();
    private final AtomicBoolean stop = new AtomicBoolean();

    public Compactor(TreeMap<Integer, Level> levels, Config aConfig) {
        config = aConfig;
        for (int i = 0; i < levels.size() - 1; i++) {
            final Level level = levels.get(i);
            final Level nextLevel = levels.get(i + 1);
            final Level nextToNextLevel = (i + 2) < levels.size() ? levels.get(i + 2) : null;
            LevelCompactor levelCompactor = new LevelCompactor(level, nextLevel, nextToNextLevel, config);
            levelCompactors.add(levelCompactor);
        }
        compactorThread = new Thread(this::compact, "compactor");
    }

    public void start() {
        compactorThread.start();
    }

    private void compact() {
        LOG.info("compaction loop started");
        while (!stop.get()) {
            if (stop.get()) break;
            try {
                pickCompactor().ifPresent(LevelCompactor::runCompaction);
                sleepSilently();
            } catch (Exception e) {
                LOG.error("caught exception in compact loop, ignoring and retrying", e);
            }
        }
        LOG.info("compaction loop exited");
    }

    private Optional<LevelCompactor> pickCompactor() {
        final TreeSet<LevelCompactor> set = new TreeSet<>(Comparator.comparing(LevelCompactor::getScore));
        set.addAll(levelCompactors);
        final LevelCompactor picked = set.last();
        return Optional.ofNullable(picked);
    }

    public void runCompaction(int levelNum) {
        levelCompactors.get(levelNum).runCompaction();
    }

    public void stop() {
        LOG.debug("stop");
        stop.set(true);
    }


    private void sleepSilently() {
        try {
            Thread.sleep(config.sleepBetweenCompactionsMs);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    static class LevelCompactor {
        private final Level level;
        private final Level nextLevel;
        private final Level nextToNextLevel;
        private final Config config;

        @Override
        public String toString() {
            return "[LevelCompactor " + level + "]";
        }

        public LevelCompactor(Level level, Level nextLevel, Level nextToNextLevel, Config config) {
            this.level = level;
            this.nextLevel = nextLevel;
            this.nextToNextLevel = nextToNextLevel;
            this.config = config;
        }

        public void runCompaction() {
            final List<Segment> fromSegments = level.markSegmentsForCompaction();
            if (fromSegments.isEmpty()) return;

            final String minKey = Collections.min(fromSegments.stream().map(Segment::getMinKey).collect(Collectors.toList()));
            final String maxKey = Collections.max(fromSegments.stream().map(Segment::getMaxKey).collect(Collectors.toList()));
            final List<Segment> overlappingSegments = nextLevel.markOverlappingSegmentsForCompaction(minKey, maxKey);

            List<Segment> toBeCompacted = addLists(fromSegments, overlappingSegments);
            if (toBeCompacted.isEmpty()) return;

            LOG.debug("compacting {}", level);
            long start = System.currentTimeMillis();
            compactSegments(toBeCompacted, nextLevel);
            fromSegments.forEach(level::removeSegment);
            overlappingSegments.forEach(nextLevel::removeSegment);
            final long timeTaken = System.currentTimeMillis() - start;
            LOG.debug("compacted {} segments in {} ms to {}: {}/{} + {}/{} - minKey {}, maxKey {}",
                    toBeCompacted.size(), timeTaken, nextLevel, fromSegments.size(), level.segmentCount(), overlappingSegments.size(), nextLevel.segmentCount(), minKey, maxKey);
        }

        private List<Segment> addLists(List<Segment> fromSegments, List<Segment> toSegments) {
            List<Segment> result = new ArrayList<>();
            result.addAll(fromSegments);
            result.addAll(toSegments);
            return result;
        }

        public void compactSegments(List<Segment> segments, Level toLevel) {
            PriorityQueue<SegmentScanner> scanners = segments.stream()
                    .map(SegmentScanner::new)
                    .filter(SegmentScanner::hasNext)
                    .collect(Collectors.toCollection(PriorityQueue::new));

            Segment segment = null;
            Segment.SegmentWriter writer = null;
            String minKey = null;
            String maxKey = null;

            do {
                SegmentScanner scanner = scanners.peek();
                if (scanner == null) {
                    break;
                } else {
                    KeyValueEntry entry = scanner.peek();

                    if (segment == null) {
                        segment = toLevel.createNextSegment();
                        writer = segment.getWriter();
                        minKey = entry.key;
                    }

                    writer.write(entry);
                    maxKey = entry.key;
                    if (writer.isFull(config.maxSegmentSize) || crossedOverlappingSegmentsThresholdOfNextToNextLevel(minKey, maxKey)) {
                        writer.done();
                        toLevel.addSegment(segment);
                        segment = null;
                        writer = null;
                    }

                    PriorityQueue<SegmentScanner> nextScanners = new PriorityQueue<>();
                    while ((scanner = scanners.poll()) != null) {
                        scanner.moveToNextIfEquals(entry.key);
                        if (scanner.hasNext()) {
                            nextScanners.add(scanner);
                        }
                    }
                    scanners = nextScanners;
                }
            } while (true);

            if (segment != null) {
                writer.done();
                toLevel.addSegment(segment);
            }
        }

        private boolean crossedOverlappingSegmentsThresholdOfNextToNextLevel(String minKey, String maxKey) {
            if (nextToNextLevel == null) return false;
            return nextToNextLevel.segmentsSpannedBy(minKey, maxKey) > 10;
        }

        public double getScore() {
            return level.getCompactionScore();
        }

        private static class SegmentScanner implements Comparable<SegmentScanner> {
            private final Segment segment;
            private final Iterator<KeyValueEntry> iterator;
            private KeyValueEntry next;

            public SegmentScanner(Segment segment) {
                this.segment = segment;
                this.iterator = segment.keyValueEntryIterator();
                if (iterator.hasNext()) {
                    next = iterator.next();
                }
            }

            @Override
            public int compareTo(SegmentScanner other) {
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
                if (key.equals(next.key)) {
                    next = iterator.hasNext() ?
                            iterator.next() :
                            null;
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

    }
}
