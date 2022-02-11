package store.ldb;

import java.util.function.Function;

public class Config {
    public static final int KB = 1024;
    public static final int MB = KB * KB;

    public final CompressionType compressionType;
    public final int maxSegmentSize;
    public final Function<Level, Integer> levelCompactionThreshold;
    public final int numLevels;
    public final int maxBlockSize;
    public final int maxWalSize;
    public final long sleepBetweenCompactionsMs;

    public static ConfigBuilder defaultConfig() {
        return builder().
                withCompressionType(CompressionType.LZ4).
                withMaxSegmentSize(2 * MB).
                withLevelCompactionThreshold(level -> level.getNum() <= 0 ? 4 : (int) Math.pow(10, level.getNum())).
                withNumLevels(4).
                withMaxBlockSize(100 * KB).
                withMaxWalSize(10 * MB).
                withSleepBetweenCompactionsMs(100);
    }

    public Config(CompressionType compressionType, int maxSegmentSize, Function<Level, Integer> levelCompactionThreshold, int numLevels, int maxBlockSize, int maxWalSize, long sleepBetweenCompactionsMs) {
        this.compressionType = compressionType;
        this.maxSegmentSize = maxSegmentSize;
        this.levelCompactionThreshold = levelCompactionThreshold;
        this.numLevels = numLevels;
        this.maxBlockSize = maxBlockSize;
        this.maxWalSize = maxWalSize;
        this.sleepBetweenCompactionsMs = sleepBetweenCompactionsMs;
    }

    @Override
    public String toString() {
        return "Config{" +
                "compressionType=" + compressionType +
                ", maxSegmentSize=" + maxSegmentSize +
                ", numLevels=" + numLevels +
                ", maxBlockSize=" + maxBlockSize +
                ", maxWalSize=" + maxWalSize +
                ", sleepBetweenCompactionsMs=" + sleepBetweenCompactionsMs +
                '}';
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
        private long sleepBetweenCompactionsMs;

        public Config build() {
            return new Config(compressionType, maxSegmentSize, levelCompactionThreshold, numLevels, maxBlockSize, maxWalSize, sleepBetweenCompactionsMs);
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

        public ConfigBuilder withSleepBetweenCompactionsMs(int ms) {
            this.sleepBetweenCompactionsMs = ms;
            return this;
        }
    }
}
