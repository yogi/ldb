package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);

    private static List<Compactor> compactors;

    private final Level level;
    private final Level nextLevel;
    private final Thread thread;
    private final Config config;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicBoolean compactionInProgress = new AtomicBoolean();
    private final Semaphore pause = new Semaphore(1);

    public static void startAll(TreeMap<Integer, Level> levels, Config config) {
        compactors = new ArrayList<>();
        for (int i = 0; i < levels.size() - 1; i++) {
            final Level level = levels.get(i);
            final Level nextLevel = levels.get(i + 1);
            Compactor compactor = new Compactor(level, nextLevel, config);
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

    public Compactor(Level level, Level nextLevel, Config config) {
        this.level = level;
        this.nextLevel = nextLevel;
        this.thread = new Thread(this::compact, "compactor-" + level.getNum());
        this.config = config;
    }

    private void compact() {
        LOG.info("started compaction loop");
        while (!stop.get()) {
            try {
                waitIfPaused();
                runCompaction();
                sleepSilently();
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

        final List<Segment> fromSegments = level.markSegmentsForCompaction();
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
            LOG.debug("compacted {} segments in {} ms to {}: {} (of {}) + {} (of {}) - minKey {}, maxKey {}",
                    toBeCompacted.size(), timeTaken, nextLevel, fromSegments.size(), level.segmentCount(), overlappingSegments.size(), nextLevel.segmentCount(), minKey, maxKey);
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
                if (writer.isFull(config.maxSegmentSize)) {
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

    private void sleepSilently() {
        try {
            Thread.sleep(config.sleepBetweenCompactionsMs);
        } catch (InterruptedException e) {
            // ignore
        }
    }

}
