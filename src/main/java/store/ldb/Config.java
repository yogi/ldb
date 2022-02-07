package store.ldb;

import java.util.function.Function;

public class Config {
    public final CompressionType compressionType;
    public final int maxSegmentSize;
    public final Function<Level, Integer> levelCompactionThreshold;
    public int numLevels;
    public final int maxBlockSize;

    public Config(CompressionType compressionType, int maxSegmentSize, Function<Level, Integer> levelCompactionThreshold, int numLevels, int maxBlockSize) {
        this.compressionType = compressionType;
        this.maxSegmentSize = maxSegmentSize;
        this.levelCompactionThreshold = levelCompactionThreshold;
        this.numLevels = numLevels;
        this.maxBlockSize = maxBlockSize;
    }

    public static ConfigBuilder builder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        private CompressionType compressionType = CompressionType.NONE;
        private int maxSegmentSize;
        private Function<Level, Integer> levelCompactionThreshold;
        private int numLevels;
        private int maxBlockSize;

        public Config build() {
            return new Config(compressionType, maxSegmentSize, levelCompactionThreshold, numLevels, maxBlockSize);
        }

        public ConfigBuilder withCompressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public ConfigBuilder withMaxSegmentSize(int limit) {
            maxSegmentSize = limit;
            return this;
        }

        public ConfigBuilder withLevelCompactionThreshold(Function<Level, Integer> f) {
            levelCompactionThreshold = f;
            return this;
        }

        public ConfigBuilder withNumLevels(int num) {
            this.numLevels = num;
            return this;
        }

        public ConfigBuilder withMaxBlockSize(int size) {
            this.maxBlockSize = size;
            return this;
        }
    }
}
