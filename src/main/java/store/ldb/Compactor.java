package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);

    private final Config config;
    private final Thread levelZeroThread;
    private final Thread otherLevelsThread;
    private final List<LevelCompactor> levelCompactors = new ArrayList<>();
    private final AtomicBoolean stop = new AtomicBoolean();
    private LevelCompactor levelZeroCompactor;
    private ExecutorService levelZeroExecutorService;
    private ExecutorService otherLevelsExecutorService;

    public Compactor(TreeMap<Integer, Level> levels, Config aConfig) {
        config = aConfig;
        for (int i = 0; i < levels.size() - 1; i++) {
            final Level level = levels.get(i);
            final Level nextLevel = levels.get(i + 1);
            final Level nextToNextLevel = (i + 2) < levels.size() ? levels.get(i + 2) : null;
            if (i == 0) {
                levelZeroCompactor = new LevelCompactor(level, nextLevel, nextToNextLevel, config);
            } else {
                LevelCompactor levelCompactor = new LevelCompactor(level, nextLevel, nextToNextLevel, config);
                levelCompactors.add(levelCompactor);
            }
        }
        levelZeroThread = new Thread(this::levelZeroDispatch, "compactor-0");
        otherLevelsThread = new Thread(this::otherLevelsPrioritize, "compactor-rest");
    }

    public void start() {
        levelZeroExecutorService = Executors.newFixedThreadPool(2);
        levelZeroThread.start();
        otherLevelsExecutorService = Executors.newFixedThreadPool(4);
        otherLevelsThread.start();
    }

    private void levelZeroDispatch() {
        LOG.info("started");
        while (!stop.get()) {
            if (stop.get()) break;
            try {
                levelZeroExecutorService.submit(levelZeroCompactor::runCompaction);
                sleepSilently();
            } catch (Exception e) {
                LOG.error("caught exception in compact loop, ignoring and retrying", e);
            }
        }
        LOG.info("exited");
    }

    private void otherLevelsPrioritize() {
        LOG.info("started");
        while (!stop.get()) {
            if (stop.get()) break;
            try {
                final Optional<LevelCompactor> lc = pickCompactor();
                lc.ifPresent(levelCompactor -> {
                    otherLevelsExecutorService.submit(levelCompactor::runCompaction);
                });
                sleepSilently();
            } catch (Exception e) {
                LOG.error("caught exception in compact loop, ignoring and retrying", e);
            }
        }
        LOG.info("exited");
    }

    private Optional<LevelCompactor> pickCompactor() {
        List<Map.Entry<LevelCompactor, Double>> list = levelCompactors.stream()
                .map(lc -> Map.entry(lc, lc.level.getCompactionScore()))
                .sorted(((Comparator<Map.Entry<LevelCompactor, Double>>) (o1, o2) -> Double.compare(o2.getValue(), o1.getValue()))
                        .thenComparing(e -> e.getKey().level.getNum()))
                .collect(Collectors.toList());
        Map.Entry<LevelCompactor, Double> picked = list.get(0);
        if (picked != null && picked.getValue() > 0) {
            //LOG.debug("picked {} from {}", picked, list);
            return Optional.of(picked.getKey());
        }
        return Optional.empty();
    }

    public void runCompaction(int levelNum) {
        LevelCompactor compactor = levelNum == 0 ?
                levelZeroCompactor :
                levelCompactors.get(levelNum - 1);
        compactor.runCompaction();
    }

    public void stop() {
        LOG.debug("stop");
        stop.set(true);
        if (levelZeroExecutorService != null) levelZeroExecutorService.shutdown();
        if (otherLevelsExecutorService != null) otherLevelsExecutorService.shutdown();
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
            return level.dirPathName();
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

            CompactionStatistics stats = new CompactionStatistics();
            long start = System.currentTimeMillis();
            if (toBeCompacted.size() == 1) {
                copySegment(toBeCompacted.get(0), nextLevel, stats);
            } else {
                compactSegments(toBeCompacted, nextLevel, stats);
            }
            fromSegments.forEach(level::removeSegment);
            overlappingSegments.forEach(nextLevel::removeSegment);
            final long timeTaken = System.currentTimeMillis() - start;
            LOG.debug("compacted {} - {} segments in {} ms - minKey {}, maxKey {}, newSegments {}, totalBytesWritten {}KB",
                    level, toBeCompacted.size(), timeTaken, minKey, maxKey, stats.newSegments, Utils.roundTo(stats.totalBytesWritten / 1024.0, 2));
        }

        private void copySegment(Segment segment, Level nextLevel, CompactionStatistics stats) {
            final Segment newSegment = nextLevel.createNextSegment();
            newSegment.copyFrom(segment);
            nextLevel.addSegment(newSegment);
            stats.incrNewSegments();
            stats.incrTotalBytesWritten(segment.totalBytes());
        }

        private List<Segment> addLists(List<Segment> fromSegments, List<Segment> toSegments) {
            List<Segment> result = new ArrayList<>();
            result.addAll(fromSegments);
            result.addAll(toSegments);
            return result;
        }

        public void compactSegments(List<Segment> segments, Level toLevel, CompactionStatistics stats) {
            Segment segment = null;
            Segment.SegmentWriter writer = null;
            String minKey = null;
            String maxKey = null;

            PriorityQueue<SegmentScanner> pendingScanners = segments.stream()
                    .map(SegmentScanner::new)
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
                    if (writer.isFull(config.maxSegmentSize) || crossedOverlappingSegmentsThresholdOfNextToNextLevel(minKey, maxKey)) {
                        writer.done();
                        toLevel.addSegment(segment);
                        stats.incrNewSegments();
                        stats.incrTotalBytesWritten(segment.totalBytes());
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
                writer.done();
                toLevel.addSegment(segment);
                stats.incrNewSegments();
                stats.incrTotalBytesWritten(segment.totalBytes());
            }
        }

        private boolean crossedOverlappingSegmentsThresholdOfNextToNextLevel(String minKey, String maxKey) {
            if (nextToNextLevel == null) return false;
            return nextToNextLevel.segmentsSpannedBy(minKey, maxKey) > 10;
        }

        private static class SegmentScanner implements Comparable<SegmentScanner> {
            private final Segment segment;
            private final KeyValueEntryIterator iterator;
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
                    next = iterator.hasNext() ?
                            iterator.next() :
                            null;
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
        int newSegments;
        long totalBytesWritten;

        public void incrNewSegments() {
            newSegments++;
        }

        public void incrTotalBytesWritten(long bytes) {
            totalBytesWritten += bytes;
        }
    }
}
