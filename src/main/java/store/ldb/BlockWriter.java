package store.ldb;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class BlockWriter {
    public static final CompressionType BLOCK_COMPRESSION_TYPE = CompressionType.NONE;
    private final List<KeyValueEntry> entries = new ArrayList<>();
    private int totalBytes;

    public Block writeTo(DataOutputStream os, int offset, String fileName) {
        try {
            byte[] data = compress(entries);
            os.write(data);
            os.flush();
            return new Block(entries.get(0).key, offset, data.length, BLOCK_COMPRESSION_TYPE, fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] compress(List<KeyValueEntry> entries) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream os = new DataOutputStream(bytes)) {
            for (KeyValueEntry keyValueEntry : entries) {
                keyValueEntry.writeTo(os);
            }
            return BLOCK_COMPRESSION_TYPE.compress(bytes.toByteArray());
        }
    }

    public boolean isFull(int maxBlockSize) {
        return totalBytes >= maxBlockSize;
    }

    public void addEntry(KeyValueEntry entry) {
        totalBytes += entry.totalBytes();
        entries.add(entry);
    }
}
