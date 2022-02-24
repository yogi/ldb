package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.abbreviate;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);

    private final Config config;
    private final Thread compactionThread;
    private final List<LevelCompactor> levelCompactors;
    private final AtomicBoolean stop = new AtomicBoolean();

    public Compactor(Levels levels, Config config, Manifest manifest) {
        this.config = config;
        this.levelCompactors = levels.createCompactors(config, manifest);
        this.compactionThread = new Thread(this::prioritize, "compaction");
    }

    public void start() {
        compactionThread.start();
    }

    private void prioritize() {
        LOG.info("compaction thread started");
        while (!stop.get()) {
            try {
                final Optional<LevelCompactor> lc = pickCompactor();
                lc.ifPresent(levelCompactor -> {
                    try {
                        levelCompactor.runCompaction();
                    } catch (Exception e) {
                        LOG.error("caught exception in runCompaction, exiting: ", e);
                        System.exit(1);
                    }
                });
                sleepSilently();
            } catch (Exception e) {
                LOG.error("caught exception in compact loop, ignoring and retrying", e);
            }
        }
        LOG.info("compaction thread exited");
    }

    private Optional<LevelCompactor> pickCompactor() {
        List<Map.Entry<LevelCompactor, Double>> list = levelCompactors.stream()
                .map(lc -> Map.entry(lc, lc.level.getCompactionScore()))
                .sorted(((Comparator<Map.Entry<LevelCompactor, Double>>) (o1, o2) -> Double.compare(o2.getValue(), o1.getValue()))
                        .thenComparing(e -> e.getKey().level.getNum()))
                .collect(Collectors.toList());
        Map.Entry<LevelCompactor, Double> picked = list.get(0);
        if (picked != null && picked.getValue() > 0) {
            //LOG.debug("pickCompactor {} from {}", picked, list);
            return Optional.of(picked.getKey());
        }
        return Optional.empty();
    }

    public void runCompaction(int levelNum) {
        levelCompactors.get(levelNum).runCompaction();
    }

    public void stop() {
        LOG.debug("stop");
        stop.set(true);
        compactionThread.interrupt();
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
        private Manifest manifest;

        @Override
        public String toString() {
            return level.dirPathName();
        }

        public LevelCompactor(Level level, Level nextLevel, Level nextToNextLevel, Config config, Manifest manifest) {
            this.level = level;
            this.nextLevel = nextLevel;
            this.nextToNextLevel = nextToNextLevel;
            this.config = config;
            this.manifest = manifest;
        }

        public void runCompaction() {
            final List<Segment> fromSegments;
            final List<Segment> overlappingSegments;
            List<Segment> toBeCompacted;
            final String minKey;
            final String maxKey;

            fromSegments = level.getSegmentsForCompaction();
            if (fromSegments.isEmpty()) return;

            minKey = Collections.min(fromSegments.stream().map(Segment::getMinKey).collect(Collectors.toList()));
            maxKey = Collections.max(fromSegments.stream().map(Segment::getMaxKey).collect(Collectors.toList()));
            overlappingSegments = nextLevel.getOverlappingSegments(null, minKey, maxKey);
            if (overlappingSegments.stream().anyMatch(Segment::isMarkedForCompaction)) return;

            toBeCompacted = addLists(fromSegments, overlappingSegments);
            if (toBeCompacted.isEmpty()) return;

            toBeCompacted.forEach(Segment::markForCompaction);

            CompactionStatistics stats = new CompactionStatistics();
            long start = System.currentTimeMillis();
            List<Segment> newlyCreated = new ArrayList<>();
            if (toBeCompacted.size() == 1) {
                copySegment(toBeCompacted.get(0), newlyCreated, nextLevel, stats);
            } else {
                compactSegments(toBeCompacted, newlyCreated, nextLevel, stats);
            }
            manifest.record(newlyCreated, toBeCompacted);
            fromSegments.forEach(level::removeSegment);
            overlappingSegments.forEach(nextLevel::removeSegment);
            final long timeTaken = System.currentTimeMillis() - start;
            LOG.debug("compacted level{}: {} + {} => {}, {}KB in {} ms - keys {} => {} - min {}, max {}",
                    level.getNum(), fromSegments.size(), overlappingSegments.size(), stats.segmentsCreated, Utils.roundTo(stats.bytesWritten / 1024.0, 2), timeTaken, stats.keysRead, stats.keysWritten, abbreviate(minKey, 15), abbreviate(maxKey, 15));
        }

        private void copySegment(Segment segment, List<Segment> newlyCreated, Level nextLevel, CompactionStatistics stats) {
            final Segment newSegment = nextLevel.createNextSegment();
            newSegment.copyFrom(segment);
            nextLevel.addSegment(newSegment);
            newlyCreated.add(newSegment);
            stats.incrSegmentsCreated();
            stats.incrBytesWritten(segment.totalBytes());
        }

        private List<Segment> addLists(List<Segment> fromSegments, List<Segment> toSegments) {
            List<Segment> result = new ArrayList<>();
            result.addAll(fromSegments);
            result.addAll(toSegments);
            return result;
        }

        public void compactSegments(List<Segment> segments, List<Segment> newlyCreated, Level toLevel, CompactionStatistics stats) {
            Segment segment = null;
            Segment.SegmentWriter writer = null;
            String minKey = null;
            String maxKey;

            PriorityQueue<SegmentScanner> pendingScanners = segments.stream()
                    .map((Segment seg) -> new SegmentScanner(seg, stats))
                    .filter(SegmentScanner::hasNext)
                    .collect(Collectors.toCollection(PriorityQueue::new));

            do {
                SegmentScanner scanner = pendingScanners.peek();
                if (scanner == null) {
                    break;
                } else {
                    KeyValueEntry entry = scanner.peek();

                    if (segment == null) {
                        segment = toLevel.createNextSegment();
                        writer = segment.getWriter();
                        minKey = entry.getKey();
                    }

                    writer.write(entry);
                    maxKey = entry.getKey();

                    final boolean writerFull = writer.isFull(config.maxSegmentSize);
                    boolean spanThresholdCrossed = false;
                    final Level.SegmentSpan span;
                    if (nextToNextLevel != null) {
                        span = nextToNextLevel.segmentSpan(minKey, maxKey);
                        if (span.count() > 10 || span.size() > (10L * config.maxSegmentSize)) {
                            spanThresholdCrossed = true;
                        }
                    }
                    if (writerFull || spanThresholdCrossed) {
                        if (writer.keyCount <= 1) {
                            LOG.debug("!!! creating new segment after only 1 entry, writerFull: {}, spanCrossed: {} - nextToNextLevel {} for range {} - {}",
                                    nextToNextLevel, writerFull, spanThresholdCrossed, minKey, maxKey);
                        }
                        flushSegment(toLevel, stats, segment, writer, newlyCreated);
                        segment = null;
                        writer = null;
                    }

                    PriorityQueue<SegmentScanner> nextScanners = new PriorityQueue<>();
                    while ((scanner = pendingScanners.poll()) != null) {
                        scanner.moveToNextIfEquals(entry.getKey());
                        if (scanner.hasNext()) {
                            nextScanners.add(scanner);
                        }
                    }
                    pendingScanners = nextScanners;
                }
            } while (true);

            if (segment != null) {
                flushSegment(toLevel, stats, segment, writer, newlyCreated);
            }
        }

        private void flushSegment(Level toLevel, CompactionStatistics stats, Segment segment, Segment.SegmentWriter writer, List<Segment> newlyCreated) {
            writer.done();
            toLevel.addSegment(segment);
            newlyCreated.add(segment);
            stats.incrSegmentsCreated();
            stats.incrBytesWritten(segment.totalBytes());
            stats.incrKeysWritten(writer.keyCount);
        }

        private static class SegmentScanner implements Comparable<SegmentScanner> {
            private final Segment segment;
            private final KeyValueEntryIterator iterator;
            private final CompactionStatistics stats;
            private KeyValueEntry next;

            public SegmentScanner(Segment segment, CompactionStatistics stats) {
                this.segment = segment;
                this.iterator = segment.keyValueEntryIterator();
                this.stats = stats;
                if (iterator.hasNext()) {
                    next = iterator.next();
                    stats.incrKeysRead();
                }
            }

            @Override
            public int compareTo(SegmentScanner other) {
                int result = peek().getKey().compareTo(other.peek().getKey());
                return result == 0 ? other.segment.num - segment.num : result;
            }

            private KeyValueEntry peek() {
                return next;
            }

            public boolean hasNext() {
                return next != null;
            }

            public void moveToNextIfEquals(String key) {
                if (key.equals(next.getKey())) {
                    if (iterator.hasNext()) {
                        stats.incrKeysRead();
                        next = iterator.next();
                    } else {
                        next = null;
                    }
                }
            }

            @Override
            public String toString() {
                return "SegmentScanner{" +
                        "segment=" + segment.num +
                        ", next=" + (next == null ? "null" : next.getKey()) +
                        '}';
            }
        }

    }

    static class CompactionStatistics {
        int segmentsCreated;
        long bytesWritten;
        int keysWritten;
        int keysRead;

        public void incrSegmentsCreated() {
            segmentsCreated++;
        }

        public void incrBytesWritten(long bytes) {
            bytesWritten += bytes;
        }

        public void incrKeysWritten(int n) {
            keysWritten += n;
        }

        public void incrKeysRead() {
            keysRead++;
        }
    }
}
