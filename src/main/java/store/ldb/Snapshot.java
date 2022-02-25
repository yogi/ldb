package store.ldb;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Snapshot {
    private final Map<Level, NavigableMap<Object, Segment>> levelToSegmentsMap;

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
            nextLevelToSegmentsMap.put(level, newSegmentsMap);
            update(newSegmentsMap, segmentsCreated, segmentsDeleted, level);
        }
        return new Snapshot(nextLevelToSegmentsMap);
    }

    private void update(ConcurrentSkipListMap<Object, Segment> newSegmentsMap, List<Segment> segmentsCreated, List<Segment> segmentsDeleted, Level level) {
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
    }
}
