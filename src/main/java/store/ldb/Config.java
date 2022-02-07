package store.ldb;

import java.util.function.Function;

public class Config {
    public final CompressionType compressionType;
    public final int maxSegmentSize;
    public final Function<Level, Integer> segmentCompactionThreshold;

    public Config(CompressionType compressionType, int maxSegmentSize, Function<Level, Integer> segmentCompactionThreshold) {
        this.compressionType = compressionType;
        this.maxSegmentSize = maxSegmentSize;
        this.segmentCompactionThreshold = segmentCompactionThreshold;
    }

    public static ConfigBuilder builder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        private CompressionType compressionType = CompressionType.NONE;
        private int maxSegmentSize;
        private Function<Level, Integer> segmentCompactionThreshold;

        public Config build() {
            return new Config(compressionType, maxSegmentSize, segmentCompactionThreshold);
        }

        public ConfigBuilder withCompressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public ConfigBuilder withMaxSegmentSize(int limit) {
            maxSegmentSize = limit;
            return this;
        }

        public ConfigBuilder withSegmentCompactionThreshold(Function<Level, Integer> f) {
            segmentCompactionThreshold = f;
            return this;
        }
    }
}
