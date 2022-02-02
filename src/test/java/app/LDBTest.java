package app;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import store.ldb.LDB;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class LDBTest {
    private final File basedir = new File("tmp/ldb");
    private LDB store;

    @BeforeEach
    void setUp() throws IOException {
        if (basedir.exists()) FileUtils.deleteDirectory(basedir);
    }

    @Test
    public void shouldFlushAndCompactWith2Levels() throws InterruptedException {
        store = new LDB(basedir.getPath(), 1, 1, 2);

        store.set("a", "b");
        assertEquals("b", store.get("a").orElseThrow());
        assertFileExists("tmp/ldb/wal0");
        assertFileDoesNotExist("tmp/ldb/level0/seg0");

        store.set("b", "c");
        Thread.sleep(100);
        assertEquals("b", store.get("a").orElseThrow());
        assertEquals("c", store.get("b").orElseThrow());
        assertFileDoesNotExist("tmp/ldb/wal0");
        assertFileExists("tmp/ldb/wal1");
        assertFileDoesNotExist("tmp/ldb/level0/seg0");
        assertFileExists("tmp/ldb/level1/seg0");
        assertFileExists("tmp/ldb/level1/seg1");
    }

    private void assertFileExists(String pathname) {
        assertTrue(new File(pathname).exists(), () -> String.format("file exists %s", pathname));
    }

    private void assertFileDoesNotExist(String pathname) {
        assertFalse(new File(pathname).exists(), () -> String.format("file exists %s", pathname));
    }

}
