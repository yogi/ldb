package store.ldb;

import java.util.function.Function;

public class Config {
    public static final int KB = 1024;
    public static final int MB = KB * KB;
    public final CompressionType compressionType;
    public final int maxSegmentSize;
    public final Function<Level, Integer> levelCompactionThreshold;
    public int numLevels;
    public final int maxBlockSize;
    public final int maxWalSize;

    public static Config defaultConfig() {
        return builder().
                withCompressionType(CompressionType.SNAPPY).
                withMaxSegmentSize(2 * MB).
                withLevelCompactionThreshold(level -> level.getNum() <= 0 ? 4 : (int) Math.pow(10, level.getNum())).
                withNumLevels(4).
                withMaxBlockSize(100 * KB).
                withMaxWalSize(4 * MB).
                build();
    }

    public Config(CompressionType compressionType, int maxSegmentSize, Function<Level, Integer> levelCompactionThreshold, int numLevels, int maxBlockSize, int maxWalSize) {
        this.compressionType = compressionType;
        this.maxSegmentSize = maxSegmentSize;
        this.levelCompactionThreshold = levelCompactionThreshold;
        this.numLevels = numLevels;
        this.maxBlockSize = maxBlockSize;
        this.maxWalSize = maxWalSize;
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
        private int maxWalSize;

        public Config build() {
            return new Config(compressionType, maxSegmentSize, levelCompactionThreshold, numLevels, maxBlockSize, maxWalSize);
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

        public ConfigBuilder withMaxWalSize(int size) {
            this.maxWalSize = size;
            return this;
        }
    }
}
