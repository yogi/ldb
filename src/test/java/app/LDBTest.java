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
    public void shouldFlushAndCompact() throws InterruptedException {
        store = new LDB(basedir.getPath(), 1, 1);

        store.set("a", "b");
        assertEquals("b", store.get("a").orElseThrow());
        assertFileExists("tmp/ldb/wal0");
        assertFileDoesNotExist("tmp/ldb/level0/seg0");

        store.set("b", "c");
        Thread.sleep(10);
        assertEquals("b", store.get("a").orElseThrow());
        assertEquals("c", store.get("b").orElseThrow());
        assertFileDoesNotExist("tmp/ldb/wal0");
        assertFileExists("tmp/ldb/wal1");
        assertFileDoesNotExist("tmp/ldb/level0/seg0");
        assertFileExists("tmp/ldb/level1/seg0");
        assertFileExists("tmp/ldb/level1/seg1");
        assertFileExists("tmp/ldb/level1/seg2");

        store.set("c", "d");
        store.set("d", "e"); // need the second set to trigger wal flush since wal was empty on previous set
        Thread.sleep(10);
        assertEquals("b", store.get("a").orElseThrow());
        assertEquals("c", store.get("b").orElseThrow());
        assertEquals("d", store.get("c").orElseThrow());
        assertEquals("e", store.get("d").orElseThrow());
        assertFileDoesNotExist("tmp/ldb/wal0");
        assertFileDoesNotExist("tmp/ldb/wal1");
        assertFileExists("tmp/ldb/wal2");
        assertFileDoesNotExist("tmp/ldb/level0/seg0");
        assertFileExists("tmp/ldb/level1/seg1");
        assertFileExists("tmp/ldb/level1/seg2");
        assertFileExists("tmp/ldb/level1/seg3");
        assertFileExists("tmp/ldb/level1/seg4");
        assertFileExists("tmp/ldb/level1/seg5");
    }

    private void assertFileExists(String pathname) {
        assertTrue(new File(pathname).exists(), () -> String.format("file exists %s", pathname));
    }

    private void assertFileDoesNotExist(String pathname) {
        assertFalse(new File(pathname).exists(), () -> String.format("file exists %s", pathname));
    }

}
