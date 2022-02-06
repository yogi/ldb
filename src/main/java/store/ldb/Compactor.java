package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
    public static final int SLEEP_BETWEEN_COMPACTIONS_MS = 1;

    private static List<Compactor> compactors;

    private final Level level;
    private final Level nextLevel;
    private final int minCompactionSegmentCount;
    private final Thread thread;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicBoolean compactionInProgress = new AtomicBoolean();
    private final Semaphore pause = new Semaphore(1);

    public static void startAll(TreeMap<Integer, Level> levels, int minCompactionSegmentCount) {
        compactors = new ArrayList<>();
        for (int i = 0; i < levels.size() - 1; i++) {
            Compactor compactor = new Compactor(levels.get(i), levels.get(i + 1), minCompactionSegmentCount);
            compactors.add(compactor);
            compactor.start();
        }
    }

    public static void stopAll() {
        compactors.forEach(Compactor::stop);
    }

    public static void pauseAll() {
        compactors.forEach(Compactor::pause);
    }

    public static void unpauseAll() {
        compactors.forEach(Compactor::unpause);
    }

    public Compactor(Level level, Level nextLevel, int minCompactionSegmentCount) {
        this.level = level;
        this.nextLevel = nextLevel;
        this.minCompactionSegmentCount = minCompactionSegmentCount;
        this.thread = new Thread(this::compact, "compactor-" + level.getNum());
    }

    private void compact() {
        LOG.info("started compaction loop");
        while (!stop.get()) {
            try {
                waitIfPaused();
                runCompaction();
                sleepSilently(SLEEP_BETWEEN_COMPACTIONS_MS);
            } catch (Exception e) {
                LOG.error("caught exception in compact loop, ignoring and retrying", e);
            }
        }
        LOG.info("compaction loop stopped");
    }

    public static void runCompaction(int levelNum) {
        compactors.get(levelNum).runCompaction();
    }

    private void start() {
        thread.start();
    }

    public void pause() {
        pause.drainPermits();
    }

    public void unpause() {
        pause.release();
    }

    private void waitIfPaused() {
        while (true) {
            try {
                pause.acquire();
                pause.release();
                return;
            } catch (InterruptedException e) {
                // retry acquire
            }
        }
    }

    public void stop() {
        stop.set(true);
    }

    void runCompaction() {
        if (compactionInProgress.get()) return;

        final List<Segment> fromSegments = level.markSegmentsForCompaction(minCompactionSegmentCount * (level.getNum() + 1) * 2);
        if (fromSegments.isEmpty()) return;

        final String minKey = Collections.min(fromSegments.stream().map(Segment::getMinKey).collect(Collectors.toList()));
        final String maxKey = Collections.max(fromSegments.stream().map(Segment::getMaxKey).collect(Collectors.toList()));
        final List<Segment> overlappingSegments = nextLevel.markOverlappingSegmentsForCompaction(minKey, maxKey);

        List<Segment> toBeCompacted = addLists(fromSegments, overlappingSegments);
        if (toBeCompacted.isEmpty()) return;

        try {
            long start = System.currentTimeMillis();
            compactionInProgress.set(true);
            compactAll(toBeCompacted, nextLevel);
            fromSegments.forEach(level::removeSegment);
            overlappingSegments.forEach(nextLevel::removeSegment);
            final long timeTaken = System.currentTimeMillis() - start;
            LOG.debug("compacted in {} ms {} segments: {} + {} (of {}) to {} - minKey {}, maxKey {}",
                    timeTaken, toBeCompacted.size(), fromSegments.size(), overlappingSegments.size(), nextLevel.segmentCount(), nextLevel, minKey, maxKey);
        } finally {
            compactionInProgress.set(false);
        }
    }

    private List<Segment> addLists(List<Segment> fromSegments, List<Segment> toSegments) {
        List<Segment> result = new ArrayList<>();
        result.addAll(fromSegments);
        result.addAll(toSegments);
        return result;
    }

    private List<Segment> takeAtMost(List<Segment> segments, int max) {
        return segments.isEmpty() ?
                Collections.emptyList() :
                segments.subList(0, Math.min(segments.size(), max));
    }

    public void compactAll(List<Segment> segments, Level toLevel) {
        PriorityQueue<SegmentScanner> scanners = segments.stream()
                .map(SegmentScanner::new)
                .filter(SegmentScanner::hasNext)
                .collect(Collectors.toCollection(PriorityQueue::new));

        Segment segment = null;
        Segment.SegmentWriter writer = null;
        do {
            SegmentScanner scanner = scanners.peek();
            if (scanner == null) {
                break;
            } else {
                if (segment == null) {
                    segment = toLevel.createNextSegment();
                    writer = segment.getWriter();
                }

                KeyValueEntry entry = scanner.peek();
                writer.write(entry);
                if (writer.isFull(toLevel.maxSegmentSize())) {
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

    private void sleepSilently(int sleepBetweenCompactionsMs) {
        try {
            Thread.sleep(sleepBetweenCompactionsMs);
        } catch (InterruptedException e) {
            // ignore
        }
    }

}
