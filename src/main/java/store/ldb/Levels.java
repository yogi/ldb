package store.ldb;

import java.nio.ByteBuffer;
import java.util.*;

import static store.ldb.Level.KEY_ASC_SEGMENT_COMPARATOR;
import static store.ldb.Level.NUM_DESC_SEGMENT_COMPARATOR;

public class Levels {
    private final TreeMap<Integer, Level> levels;

    public Levels(String dir, Config config, Manifest manifest) {
        levels = new TreeMap<>();
        for (int i = 0; i < config.numLevels; i++) {
            final Comparator<Segment> segmentComparator = i == 0 ? NUM_DESC_SEGMENT_COMPARATOR : KEY_ASC_SEGMENT_COMPARATOR;
            Level level = new Level(dir, i, segmentComparator, config, manifest);
            levels.put(i, level);
        }
    }

    public Level levelZero() {
        return levels.get(0);
    }

    public double getCompactionScore() {
        return levelZero().getCompactionScore();
    }

    public List<Compactor.LevelCompactor> createCompactors(Config config, Manifest manifest) {
        List<Compactor.LevelCompactor> levelCompactors = new ArrayList<>();
        for (int i = 0; i < levels.size() - 1; i++) {
            final Level level = levels.get(i);
            final Level nextLevel = levels.get(i + 1);
            final Level nextToNextLevel = (i + 2) < levels.size() ? levels.get(i + 2) : null;
            Compactor.LevelCompactor levelCompactor = new Compactor.LevelCompactor(level, nextLevel, nextToNextLevel, config, manifest);
            levelCompactors.add(levelCompactor);
        }
        return levelCompactors;
    }

    public Optional<String> getValue(String key, ByteBuffer keyBuf) {
        for (Level level : levels.values()) {
            Optional<String> value = level.get(key, keyBuf);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    public long keyCount() {
        return levels.values().stream().mapToLong(Level::keyCount).sum();
    }

    public long totalBytes() {
        return levels.values().stream().mapToLong(Level::totalBytes).sum();
    }

    public void addStats(Map<String, Object> stats) {
        levels.values().forEach(level -> level.addStats(stats));
    }
}
