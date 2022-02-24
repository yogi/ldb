package store.ldb;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class BlockWriter {
    private final List<KeyValueEntry> entries = new ArrayList<>();
    private final Config config;
    private final Segment segment;
    private int totalBytes;

    public BlockWriter(Config config, Segment segment) {
        this.config = config;
        this.segment = segment;
    }

    public Block writeTo(DataOutputStream os, int offset) {
        try {
            byte[] data = compress(entries);
            os.write(data);
            os.flush();
            return new Block(entries.get(0).getKey(), offset, data.length, config.compressionType, segment);
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
            return config.compressionType.compress(bytes.toByteArray());
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
