package app;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import store.ldb.LDB;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class LDBTest {
    private LDB store;

    @BeforeEach
    void setUp() throws IOException {
        final File file = new File("tmp/ldb");
        if (file.exists()) FileUtils.deleteDirectory(file);
        store = new LDB(file.getPath(), 1);
    }

    @Test
    public void shouldBeAbleToGetWhatYouSet() {
        store.set("a", "b");
        assertEquals("b", store.get("a").orElseThrow());
        assertTrue(new File("tmp/ldb/wal0").exists());
        assertFalse(new File("tmp/ldb/level0/seg0").exists());

        store.set("b", "c");
        assertEquals("b", store.get("a").orElseThrow());
        assertEquals("c", store.get("b").orElseThrow());
        assertFalse(new File("tmp/ldb/wal0").exists());
        assertTrue(new File("tmp/ldb/wal1").exists());
        assertTrue(new File("tmp/ldb/level0/seg0").exists());
    }

}
