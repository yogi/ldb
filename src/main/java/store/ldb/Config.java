package store.ldb;

public class Config {
    public final CompressionType compressionType;

    public Config(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public static ConfigBuilder builder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        private CompressionType compressionType = CompressionType.NONE;

        public Config build() {
            return new Config(compressionType);
        }

        public ConfigBuilder withCompressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }
    }
}
