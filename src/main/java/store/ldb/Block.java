package store.ldb;

import org.apache.commons.io.input.CountingInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.String.format;

class Block {
    final int offset;
    final int length;
    final String startKey;
    private final CompressionType compressionType;
    private final Segment segment;

    public Block(String startKey, int offset, int length, CompressionType compressionType, Segment segment) {
        this.startKey = startKey;
        this.offset = offset;
        this.length = length;
        this.compressionType = compressionType;
        this.segment = segment;
    }

    public static int writeIndex(DataOutputStream os, Collection<Block> blocks) {
        try {
            int bytesWritten = 0;
            for (Block block : blocks) {
                bytesWritten += writeIndexEntry(block, os);
            }
            return bytesWritten;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int writeIndexEntry(Block block, DataOutputStream os) throws IOException {
        os.writeShort(block.startKey.length());
        os.write(block.startKey.getBytes());
        os.writeInt(block.offset);
        os.writeInt(block.length);
        os.writeByte(block.compressionType.code);
        os.flush();
        return Short.BYTES + block.startKey.length() + Integer.BYTES + Integer.BYTES + Byte.BYTES;
    }

    private static Block readIndexEntry(DataInputStream is, Segment segment) throws IOException {
        short startKeyLen = is.readShort();
        String startKey = new String(is.readNBytes(startKeyLen));
        int blockOffset = is.readInt();
        int blockLength = is.readInt();
        CompressionType compression = CompressionType.fromCode(is.readByte());
        return new Block(startKey, blockOffset, blockLength, compression, segment);
    }

    public static ConcurrentSkipListMap<String, Block> loadBlocks(long offset, long uptoOffset, String fileName, Segment segment) {
        try (CountingInputStream countingStream = new CountingInputStream(new BufferedInputStream(new FileInputStream(fileName)));
             DataInputStream is = new DataInputStream(countingStream)) {
            final long skippedTo = is.skip(offset);
            if (skippedTo != offset) {
                throw new IllegalStateException(format("skipped to %d instead of offset %d when loading blocks for segment %s", skippedTo, offset, fileName));
            }
            ConcurrentSkipListMap<String, Block> blocks = new ConcurrentSkipListMap<>();
            while (countingStream.getCount() < uptoOffset) {
                final Block block = readIndexEntry(is, segment);
                blocks.put(block.startKey, block);
            }
            if (countingStream.getCount() != uptoOffset) {
                throw new IllegalStateException(format("unexpected block bytes read: %d != %s", countingStream.getCount(), uptoOffset));
            }
            return blocks;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> get(String key, ByteBuffer keyBuf) {
        try {
            ByteBuffer blockBytes = ByteBuffer.wrap(uncompress());
            while(blockBytes.hasRemaining()) {
                Optional<KeyValueEntry> entry = KeyValueEntry.getIfMatches(blockBytes, keyBuf);
                if (entry.isPresent()) {
                    return Optional.of(entry.get().getValue());
                }
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<KeyValueEntry> loadAllEntries() {
        try (DataInputStream is = new DataInputStream(new ByteArrayInputStream(uncompress()))) {
            List<KeyValueEntry> entries = new ArrayList<>();
            while (is.available() > 0) {
                entries.add(KeyValueEntry.readFrom(is));
            }
            return entries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] uncompress() throws IOException {
        try (DataInputStream is = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(segment.getData().array())))) {
            long read = is.skip(offset);
            if (read != offset) {
                throw new IOException(format("skipped %d bytes vs %d for block %s", read, offset, this));
            }
            byte[] bytes = new byte[length];
            read = is.read(bytes);
            if (read != length) {
                throw new IOException(format("read %d bytes vs %d for block %s", read, length, this));
            }
            return compressionType.uncompress(bytes);
        }
    }

    @Override
    public String toString() {
        return "Block{" +
                "offset=" + offset +
                ", length=" + length +
                ", startKey='" + startKey + '\'' +
                ", compressionType=" + compressionType +
                '}';
    }

}
