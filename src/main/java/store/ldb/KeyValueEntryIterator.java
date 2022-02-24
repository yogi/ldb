package store.ldb;

import java.util.*;

class KeyValueEntryIterator implements Iterator<KeyValueEntry> {
    private final Queue<KeyValueEntry> entries;
    private final Iterator<Block> blockIterator;

    public KeyValueEntryIterator(Collection<Block> blocks) {
        entries = new LinkedList<>();
        blockIterator = blocks.iterator();
    }

    @Override
    public boolean hasNext() {
        return !entries.isEmpty() || blockIterator.hasNext();
    }

    @Override
    public KeyValueEntry next() {
        if (entries.isEmpty()) {
            entries.addAll(blockIterator.next().loadAllEntries());
        }
        return entries.poll();
    }
}
