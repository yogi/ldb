package store;

import java.util.Optional;

public interface Store {
    Optional<String> get(String key);

    void set(String key, String value);

    String stats();
}
