package store.ldb;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class Memtable {
    private ConcurrentSkipListMap<String, String> actual = new ConcurrentSkipListMap<>();

    public void put(String key, String value) {
        actual.put(key, value);
    }

    public boolean contains(String key) {
        return actual.containsKey(key);
    }

    public int size() {
        return actual.size();
    }

    public String get(String key) {
        return actual.get(key);
    }

    public List<List<Map.Entry<String, String>>> partitions(int numPartitions) {
        final List<Map.Entry<String, String>> original = new ArrayList<>(actual.entrySet());
        return original.size() < numPartitions ?
                List.of(original) :
                Lists.partition(original, original.size() / numPartitions);
    }
}
