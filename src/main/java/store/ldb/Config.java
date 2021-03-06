package store.ldb;

import java.util.List;
import java.util.function.Function;

import static store.ldb.CompressionType.*;

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
    public int memtablePartitions;
    public boolean randomizedKeys;
    public int segmentCacheSize;
    public boolean enableThrottling;

    public static Config defaultConfig() {
        final Config config = new Config();
        final CompressionType compressionType = NONE;
        int compressionFactorEstimate = List.of(LZ4, SNAPPY).contains(compressionType) ? 8 : 1;
        return config.
                withMemtablePartitions(6).
                withCompressionType(compressionType).
                withMaxSegmentSize(2 * MB).
                withSegmentCacheSize(100 * MB).
                withLevelCompactionThreshold(level -> level.getNum() == 0 ? 4 : (int) Math.pow(5, level.getNum())).
                withNumLevels(7).
                withRandomizedKeys(true).
                withMaxBlockSize(100 * KB * compressionFactorEstimate).
                withMaxWalSize(4 * MB * config.memtablePartitions * compressionFactorEstimate).
                withSleepBetweenCompactionsMs(1).
                withEnableThrottling(true)
                ;
    }

    public Config withEnableThrottling(boolean b) {
        this.enableThrottling = b;
        return this;
    }

    public Config withSegmentCacheSize(int sizeBytes) {
        segmentCacheSize = sizeBytes;
        return this;
    }

    public Config withMemtablePartitions(int memtablePartitions) {
        this.memtablePartitions = memtablePartitions;
        return this;
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

    public Config withRandomizedKeys(boolean b) {
        randomizedKeys = b;
        return this;
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
}