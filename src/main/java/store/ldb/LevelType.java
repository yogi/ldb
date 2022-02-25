package store.ldb;

import org.apache.commons.lang3.tuple.Pair;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.hasCause;
import static store.ldb.StringUtils.isGreaterThan;
import static store.ldb.Utils.roundTo;
import static store.ldb.Utils.shouldNotGetHere;

enum LevelType {
    LEVEL_0 {
        @Override
        public Comparator<Object> comparator() {
            return Comparator.comparingInt(o -> (Integer) o).reversed();
        }

        @Override
        public Object key(Segment segment) {
            return segment.getNum();
        }

        @Override
        public void assertSegmentInvariants(ConcurrentSkipListMap<Object, Segment> segments) {
            // nothing to check
        }

        @Override
        public double getCompactionScore(List<Segment> segmentsNotBeingCompacted, Config config, Level level) {
            return roundTo(segmentsNotBeingCompacted.size() / (double) config.memtablePartitions, 3);
        }

        @Override
        public Optional<String> getValue(String key, ByteBuffer keyBuf, NavigableMap<Object, Segment> segments) {
            for (Segment segment : segments.values()) {
                if (!segment.isKeyInRange(key)) continue;
                Optional<String> value = getValueSafely(key, keyBuf, segment);
                if (value.isPresent()) return value;
            }
            return Optional.empty();
        }
    },

    LEVEL_N {
        @Override
        public Comparator<Object> comparator() {
            return Comparator.comparing(o -> ((Pair<String, Integer>) o).getLeft()).thenComparing(o -> ((Pair<String, Integer>) o).getRight());
        }

        @Override
        public Object key(Segment segment) {
            return Pair.of(segment.getMinKey(), segment.getNum());
        }

        @Override
        public void assertSegmentInvariants(ConcurrentSkipListMap<Object, Segment> segments) {
            assertNoOverlappingSegments(segments);
        }

        @Override
        public double getCompactionScore(List<Segment> segmentsNotBeingCompacted, Config config, Level level) {
            // not using streams api because its showing up as a bottleneck in the profiler
            long totalBytes = 0L;
            for (Segment segment : segmentsNotBeingCompacted) {
                totalBytes += segment.totalBytes();
            }
            double thresholdBytes = config.levelCompactionThreshold.apply(level) * config.maxSegmentSize;
            final double score = (double) totalBytes / thresholdBytes;
            return roundTo(score, 3);
        }

        @Override
        public Optional<String> getValue(String key, ByteBuffer keyBuf, NavigableMap<Object, Segment> segments) {
            Map.Entry<Object, Segment> segmentEntry = segments.floorEntry(key(key));
            if (segmentEntry == null) return Optional.empty();
            Segment segment = segmentEntry.getValue();
            return getValueSafely(key, keyBuf, segment);
        }

        private Object key(String key) {
            return Pair.of(key, Integer.MAX_VALUE); // we need to find the floorEntry for this key in the NavigableMap, so pass a large value for the num part so that equal keys match
        }

        private void assertNoOverlappingSegments(ConcurrentSkipListMap<Object, Segment> segments) {
            // ensure no overlapping segments
            final List<Segment> list = new ArrayList<>(segments.values());
            for (int i = 0; i < list.size() - 1; i++) {
                Segment segment = list.get(i);
                Segment nextSegment = list.get(i + 1);
                if (segment.isMarkedForCompaction() || nextSegment.isMarkedForCompaction()) continue;
                if (isGreaterThan(segment.getMaxKey(), nextSegment.getMinKey())) {
                    shouldNotGetHere(format("found overlapping segments %s, %s", segment, nextSegment));
                }
            }
        }
    };

    public static LevelType of(int num) {
        return num == 0 ? LEVEL_0 : LEVEL_N;
    }

    public static LevelType of(Level level) {
        return of(level.getNum());
    }

    public abstract Comparator<Object> comparator();

    public abstract Object key(Segment segment);

    public abstract void assertSegmentInvariants(ConcurrentSkipListMap<Object, Segment> segments);

    public abstract double getCompactionScore(List<Segment> segmentsNotBeingCompacted, Config config, Level level);

    public abstract Optional<String> getValue(String key, ByteBuffer keyBuf, NavigableMap<Object, Segment> segments);

    private static Optional<String> getValueSafely(String key, ByteBuffer keyBuf, Segment segment) {
        try {
            return segment.get(key, keyBuf);
        } catch (RuntimeException e) {
            if (hasCause(e, FileNotFoundException.class)) {
                Level.LOG.error("ignoring error in Segment.get(), which is caused by concurrent deletion of segment {} during compaction, there will be a higher segment present with the required data", segment, e);
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

}
