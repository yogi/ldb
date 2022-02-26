package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Snapshot {
    public static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);

    private final Map<Level, NavigableMap<Object, Segment>> levelToSegmentsMap;
    private List<Segment> deletedSegments = new ArrayList<>();
    private final AtomicInteger users = new AtomicInteger();

    public Snapshot(Map<Level, NavigableMap<Object, Segment>> levelToSegmentsMap) {
        this.levelToSegmentsMap = levelToSegmentsMap;
    }

    public NavigableMap<Object, Segment> segmentsOf(Level level) {
        return levelToSegmentsMap.get(level);
    }

    public Snapshot createNextSnapshot(List<Segment> segmentsCreated, List<Segment> segmentsDeleted) {
        ConcurrentHashMap<Level, NavigableMap<Object, Segment>> nextLevelToSegmentsMap = new ConcurrentHashMap<>();
        for (Map.Entry<Level, NavigableMap<Object, Segment>> entry : this.levelToSegmentsMap.entrySet()) {
            Level level = entry.getKey();
            NavigableMap<Object, Segment> oldSegmentsMap = entry.getValue();

            ConcurrentSkipListMap<Object, Segment> newSegmentsMap = new ConcurrentSkipListMap<>(oldSegmentsMap);
            for (Segment created : segmentsCreated) {
                if (created.belongsTo(level)) {
                    newSegmentsMap.put(LevelType.of(level).key(created), created);
                }
            }
            for (Segment deleted : segmentsDeleted) {
                if (deleted.belongsTo(level)) {
                    newSegmentsMap.remove(LevelType.of(level).key(deleted));
                }
            }
            nextLevelToSegmentsMap.put(level, newSegmentsMap);
        }
        return new Snapshot(nextLevelToSegmentsMap);
    }

    public void acquire() {
        users.incrementAndGet();
    }

    public void release() {
        if (users.decrementAndGet() == 0 ) {
            if (deletedSegments == null || deletedSegments.isEmpty()) return;
            LOG.debug("releasing snapshot and deleting {} segments {}", deletedSegments.size(), deletedSegments);
            for (Segment segment : deletedSegments) {
                for (Level level : levelToSegmentsMap.keySet()) {
                    if (segment.belongsTo(level)) level.removeSegment(segment);
                }
            }
        }
    }

    public void deleteSegmentsOnFinalization(List<Segment> segmentsDeleted) {
        deletedSegments = new ArrayList<>(segmentsDeleted);
    }
}
