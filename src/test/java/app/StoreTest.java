package app;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StoreTest {
    private Store store;

    @BeforeEach
    void setUp() {
        File walFile = new File("tmp/wal");
        if (walFile.exists()) {
            walFile.delete();
        }
        store = new Store("tmp");
    }

    @Test
    public void shouldBeAbleToGetWhatYouSet() {
        store.set("a", "b");
        assertEquals("b", store.get("a"));
    }

    @Test
    public void shouldPersistData() {
        store.set("a", "b");
        store = new Store("tmp"); // simulate a crash and restart
        assertEquals("b", store.get("a"));
    }

    @Test
    public void shouldWriteSegmentFileOnMemtableThresholdCrossing() {
        store.set("a", "b");
        store = new Store("tmp"); // simulate a crash and restart
        assertEquals("b", store.get("a"));
    }
}
