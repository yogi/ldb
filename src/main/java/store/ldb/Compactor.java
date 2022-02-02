package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
    public static final int SLEEP_BETWEEN_COMPACTIONS_MS = 10;

    private final TreeMap<Integer, Level> levels;
    private final int minCompactionSegmentCount;
    private final Thread thread;
    private final AtomicBoolean stop;
    private final AtomicBoolean compactionInProgress = new AtomicBoolean();

    public Compactor(TreeMap<Integer, Level> levels, int minCompactionSegmentCount) {
        this.levels = levels;
        this.minCompactionSegmentCount = minCompactionSegmentCount;
        this.stop = new AtomicBoolean(false);
        this.thread = new Thread(this::compact, "compactor");
    }

    private void compact() {
        LOG.info("started compaction loop");
        while (!stop.get()) {
            try {
                for (int i = 0; i < levels.values().size() - 1; i++) {
                    Level level = levels.get(i);
                    final Level nextLevel = levels.get(i + 1);
                    compactLevel(level, nextLevel);
                    sleepSilently();
                }
            } catch (Exception e) {
                LOG.error("caught exception in compact loop, ignoring and retrying", e);
            }
        }
        LOG.info("compaction loop stopped");
    }

    private void compactLevel(Level fromLevel, Level toLevel) {
        LOG.debug("compact level {} to {}", fromLevel, toLevel);

        if (compactionInProgress.get()) return;

        final List<Segment> fromSegments = takeAtMost(fromLevel.getSegmentsToCompact(), 10);
        final List<Segment> toSegments = toLevel.getOverlappingSegments(fromSegments);

        List<Segment> toBeCompacted = addLists(fromSegments, toSegments);
        if (toBeCompacted.size() < minCompactionSegmentCount || toBeCompacted.stream().anyMatch(s -> !s.isReady())) {
            LOG.debug("skipping... compact level {} to {}, toBeCompacted {}", fromLevel, toLevel, toBeCompacted.size());
            return;
        }

        try {
            compactionInProgress.set(true);
            compactAll(fromSegments, toLevel);
            fromSegments.forEach(fromLevel::removeSegment);
            toSegments.forEach(toLevel::removeSegment);
            LOG.debug("done... compact level {} to {}, toBeCompacted {}", fromLevel, toLevel, toBeCompacted.size());
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
        LOG.info("compacting segments {} to level {}", segments, toLevel);

        long start = System.currentTimeMillis();

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

        if (segment !=  null) {
            writer.done();
            toLevel.addSegment(segment);
        }

        LOG.info("compacted segments {} to level {} in {} ms", segments.size(), toLevel, System.currentTimeMillis() - start);
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

    private void sleepSilently() {
        try {
            Thread.sleep(SLEEP_BETWEEN_COMPACTIONS_MS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static Compactor start(TreeMap<Integer, Level> levels, int minCompactionSegmentCount) {
        final Compactor compactor = new Compactor(levels, minCompactionSegmentCount);
        compactor.start();
        return compactor;
    }

    private void start() {
        thread.start();
    }

    public void stop() {
        stop.set(true);
    }
}
