package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
    public static final int SLEEP_BETWEEN_COMPACTIONS_MS = 10;

    private final TreeMap<Integer, Level> levels;
    private final int minCompactionSegmentCount;
    private final Thread thread;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicBoolean compactionInProgress = new AtomicBoolean();
    private final Semaphore pause = new Semaphore(1);

    public static Compactor start(TreeMap<Integer, Level> levels, int minCompactionSegmentCount) {
        final Compactor compactor = new Compactor(levels, minCompactionSegmentCount);
        compactor.start();
        return compactor;
    }

    public Compactor(TreeMap<Integer, Level> levels, int minCompactionSegmentCount) {
        this.levels = levels;
        this.minCompactionSegmentCount = minCompactionSegmentCount;
        this.thread = new Thread(this::compact, "compactor");
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
        while(true) {
            try {
                pause.acquire();
                pause.release();
                return;
            } catch (InterruptedException e) {
                ; // retry acquire
            }
        }
    }

    public void stop() {
        stop.set(true);
    }

    private void compact() {
        LOG.info("started compaction loop");
        while (!stop.get()) {
            try {
                for (int i = 0; i < levels.values().size() - 1; i++) {
                    waitIfPaused();
                    runCompaction(i);
                    sleepSilently(SLEEP_BETWEEN_COMPACTIONS_MS);
                }
            } catch (Exception e) {
                LOG.error("caught exception in compact loop, ignoring and retrying", e);
            }
        }
        LOG.info("compaction loop stopped");
    }

    void runCompaction(int levelNum) {
        Level level = levels.get(levelNum);
        final Level nextLevel = levels.get(levelNum + 1);
        compactLevel(level, nextLevel);
    }

    private void compactLevel(Level fromLevel, Level toLevel) {
        if (compactionInProgress.get()) return;

        final List<Segment> fromSegments = takeAtMost(fromLevel.getSegmentsForCompaction(), minCompactionSegmentCount);
        if (fromSegments.isEmpty()) return;

        final String minKey = Collections.min(fromSegments.stream().map(Segment::getMinKey).collect(Collectors.toList()));
        final String maxKey = Collections.max(fromSegments.stream().map(Segment::getMaxKey).collect(Collectors.toList()));
        final List<Segment> overlappingSegments = toLevel.getOverlappingSegments(minKey, maxKey);

        List<Segment> toBeCompacted = addLists(fromSegments, overlappingSegments);
        if (toBeCompacted.size() < minCompactionSegmentCount) return;

        LOG.debug("compact {} + {} overlapping-segments from level {} to {} - minKey {}, maxKey {}",
                fromSegments.size(), overlappingSegments.size(), fromLevel, toLevel, minKey, maxKey);
        try {
            long start = System.currentTimeMillis();
            compactionInProgress.set(true);
            compactAll(toBeCompacted, toLevel);
            fromSegments.forEach(fromLevel::removeSegment);
            overlappingSegments.forEach(toLevel::removeSegment);
            final long timeTaken = System.currentTimeMillis() - start;
            LOG.debug("compaction done total-segments {} in {} ms", toBeCompacted.size(), timeTaken);
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
                if (writer.writtenBytes() > toLevel.maxSegmentSize()) {
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
        private final DataInputStream is;
        private KeyValueEntry next;

        public SegmentScanner(Segment segment) {
            try {
                this.segment = segment;
                is = new DataInputStream(new BufferedInputStream(new FileInputStream(segment.fileName)));
                if (is.available() > 0) {
                    next = KeyValueEntry.readFrom(is);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            try {
                if (key.equals(next.key)) {
                    if (is.available() > 0) {
                        next = KeyValueEntry.readFrom(is);
                    } else {
                        is.close();
                        next = null;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
