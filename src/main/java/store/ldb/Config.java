package store.ldb;

import java.util.function.Function;

public class Config {
    public static final int KB = 1024;
    public static final int MB = KB * KB;

    public CompressionType compressionType;
    public int maxSegmentSize;
    public Function<Level, Integer> levelCompactionThreshold;
    public int numLevels;
    public int maxBlockSize;
    public int maxWalSize;
    public long sleepBetweenCompactionsMs;

    public static Config defaultConfig() {
        return new Config().
                withCompressionType(CompressionType.LZ4).
                withMaxSegmentSize(2 * MB).
                withLevelCompactionThreshold(level -> level.getNum() <= 0 ? 4 : (int) Math.pow(10, level.getNum())).
                withNumLevels(3).
                withMaxBlockSize(100 * KB).
                withMaxWalSize(4 * MB).
                withSleepBetweenCompactionsMs(100);
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

    public Config withCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public Config withMaxSegmentSize(int limit) {
        maxSegmentSize = limit;
        return this;
    }

    public Config withLevelCompactionThreshold(Function<Level, Integer> f) {
        levelCompactionThreshold = f;
        return this;
    }

    public Config withNumLevels(int num) {
        this.numLevels = num;
        return this;
    }

    public Config withMaxBlockSize(int size) {
        this.maxBlockSize = size;
        return this;
    }

    public Config withMaxWalSize(int size) {
        this.maxWalSize = size;
        return this;
    }

    public Config withSleepBetweenCompactionsMs(int ms) {
        this.sleepBetweenCompactionsMs = ms;
        return this;
    }
}