package app;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Store {
    Map<String, String> memtable = new ConcurrentHashMap<>();
    private WriteAheadLog wal;

    public Store(String dir) {
        this.wal = new WriteAheadLog(dir);
        wal.replay(this);
    }

    public String get(String key) {
        return memtable.get(key);
    }

    public void set(String key, String value) {
        wal.append(new SetCmd(key, value));
        setRaw(key, value);
    }

    void setRaw(String key, String value) {
        memtable.put(key, value);
    }
}
