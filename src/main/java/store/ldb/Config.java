package store.ldb;

public class Config {
    public final CompressionType compressionType;
    public final int maxSegmentSize;

    public Config(CompressionType compressionType, int maxSegmentSize) {
        this.compressionType = compressionType;
        this.maxSegmentSize = maxSegmentSize;
    }

    public static ConfigBuilder builder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        private CompressionType compressionType = CompressionType.NONE;
        private int maxSegmentSize;

        public Config build() {
            return new Config(compressionType, maxSegmentSize);
        }

        public ConfigBuilder withCompressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public ConfigBuilder withMaxSegmentSize(int limit) {
            maxSegmentSize = limit;
            return this;
        }
    }
}
