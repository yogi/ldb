package store.ldb;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.lang.String.format;

public enum CompressionType {
    NONE((byte) 1) {
        @Override
        public ByteBuffer uncompress(ByteBuffer buf) {
            return buf;
        }

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
        public ByteBuffer uncompress(ByteBuffer buf) {
            throw new UnsupportedOperationException("todo");
        }

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
    LZ4((byte) 3) {
        private final LZ4Factory factory = LZ4Factory.fastestJavaInstance();

        @Override
        public ByteBuffer uncompress(ByteBuffer buf) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public byte[] uncompress(byte[] compressed) {
            final LZ4FastDecompressor decompressor = factory.fastDecompressor();
            int ch1 = compressed[0];
            int ch2 = compressed[1];
            int ch3 = compressed[2];
            int ch4 = compressed[3];
            int uncompressedSize = ((0xFF & ch1) << 24) | ((0xFF & ch2) << 16) | ((0xFF & ch3) << 8) | (0xFF & ch4);
            byte[] uncompressed = new byte[uncompressedSize];
            final int compressedSizeRead = decompressor.decompress(compressed, 4, uncompressed, 0, uncompressedSize);
            if (compressedSizeRead != compressed.length - 4) {
                Utils.shouldNotGetHere(format("compressedSizeRead %d, expected %d", compressedSizeRead, compressed.length - 4));
            }
            return uncompressed;
        }

        @Override
        public byte[] compress(byte[] uncompressed) {
            final LZ4Compressor compressor = factory.fastCompressor();
            byte[] compressed = new byte[compressor.maxCompressedLength(uncompressed.length) + 4];
            compressed[0] = (byte) (uncompressed.length >>> 24);
            compressed[1] = (byte) (uncompressed.length >>> 16);
            compressed[2] = (byte) (uncompressed.length >>> 8);
            compressed[3] = (byte) (uncompressed.length >>> 0);
            int compressedSize = compressor.compress(uncompressed, 0, uncompressed.length, compressed, 4, compressed.length - 4);
            return Arrays.copyOf(compressed, compressedSize + 4);
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
        } else if (LZ4.code == b) {
            return LZ4;
        } else {
            throw new IllegalArgumentException("invalid CompressionType: " + b);
        }
    }

    public abstract byte[] uncompress(byte[] bytes);

    public abstract byte[] compress(byte[] bytes);

    public abstract ByteBuffer uncompress(ByteBuffer buf);
}
