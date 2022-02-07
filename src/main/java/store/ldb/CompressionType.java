package store.ldb;

import org.xerial.snappy.Snappy;

import java.io.IOException;

public enum CompressionType {
    NONE((byte) 1) {
        @Override
        public byte[] uncompress(byte[] bytes) {
            return bytes;
        }

        @Override
        public byte[] compress(byte[] bytes) {
            return bytes;
        }
    },
    SNAPPY((byte) 2) {
        @Override
        public byte[] uncompress(byte[] bytes) {
            try {
                return Snappy.uncompress(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] compress(byte[] bytes) {
            try {
                return Snappy.compress(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    },
    ;
    final byte code;

    CompressionType(byte code) {
        this.code = code;
    }

    public static CompressionType fromCode(byte b) {
        if (NONE.code == b) {
            return NONE;
        } else if (SNAPPY.code == b) {
            return SNAPPY;
        } else {
            throw new IllegalArgumentException("invalid CompressionType: " + b);
        }
    }

    public abstract byte[] uncompress(byte[] bytes);

    public abstract byte[] compress(byte[] bytes);
}
