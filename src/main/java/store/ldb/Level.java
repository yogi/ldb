package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Level {
    public static final Logger LOG = LoggerFactory.getLogger(Level.class);
    public static final int MB = 1024 * 1024;

    private final File dir;
    private final int num;
    private final LinkedBlockingDeque<Segment> segments;
    private final ReentrantLock lock;
    private AtomicBoolean compactionInProgress = new AtomicBoolean();

    public Level(String dirName, int num) {
        LOG.info("loading level: {}", num);
        this.num = num;
        this.dir = new File(levelDirName(dirName, num));
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("couldn't create level dir: " + num);
            }
        }

        this.segments = loadSegments(dir);
        lock = new ReentrantLock();
    }

    public static LinkedBlockingDeque<Segment> loadSegments(File dir) {
        final Comparator<Object> comparator = Comparator.comparingInt(value -> (Integer) value).reversed();
        return Arrays.stream(Objects.requireNonNull(dir.listFiles(pathname -> pathname.getName().startsWith("seg"))))
                .map(file -> Integer.parseInt(file.getName().replace("seg", "")))
                .sorted(comparator)
                .map(n -> {
                    final Segment segment = new Segment(dir, n);
                    segment.loadIndex();
                    return segment;
                })
                .collect(Collectors.toCollection(LinkedBlockingDeque::new));
    }

    private String levelDirName(String dir, int num) {
        return dir + File.separatorChar + "level" + num;
    }

    static TreeMap<Integer, Level> loadLevels(String dir) {
        TreeMap<Integer, Level> levels = new TreeMap<>();
        for (int i = 0; i < 2; i++) {
            Level level = new Level(dir, i);
            levels.put(i, level);
        }
        return levels;
    }

    public Optional<String> get(String key) {
        for (Segment segment : segments) {
            final Optional<String> value = segment.get(key);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    public void flushMemtable(TreeMap<String, String> memtable) {
        final Segment segment;
        lock.lock();
        try {
            int segmentNumber = nextSegmentNumber();
            segment = new Segment(dir, segmentNumber);
            segments.addFirst(segment);
        } finally {
            lock.unlock();
        }
        segment.writeMemtable(memtable);
    }

    private int nextSegmentNumber() {
        if (segments.isEmpty()) {
            return 0;
        }
        return segments.getFirst().getNum() + 1;
    }

    public String dirPathName() {
        return dir.getPath();
    }

    public long keyCount() {
        return segments.stream().mapToLong(Segment::keyCount).sum();
    }

    public int getNum() {
        return num;
    }

    public void compact() {
        if (compactionInProgress.get()) {
            return;
        }

        try {
            lock.lock();
            final Segment segment;
            final List<Segment> segmentsToCompact = new ArrayList<>(new ArrayList<>(segments));
            try {
                if (segmentsToCompact.size() < 10) {
                    return;
                }
                if (segmentsToCompact.stream().anyMatch(s -> !s.isReady())) {
                    return;
                }
                compactionInProgress.set(true);
                int segmentNumber = nextSegmentNumber();
                segment = new Segment(dir, segmentNumber);
                segments.addFirst(segment);
            } finally {
                lock.unlock();
            }

            LOG.info("compacting level: {}, segments: {}", num, segmentsToCompact.size());
            segment.compactAll(segmentsToCompact);

            lock.lock();
            try {
                for (Segment seg : segmentsToCompact) {
                    removeSegment(seg);
                    seg.delete();
                }
            } finally {
                lock.unlock();
            }
        } finally {
            compactionInProgress.set(false);
        }
    }

    private void removeSegment(Segment segment) {
        if (!segments.remove(segment)) {
            throw new IllegalStateException("could not remove segment: " + segment);
        }
    }

    public long maxSegmentSize() {
        return ((long) Math.pow(10, num + 1)) * MB;
    }
}
