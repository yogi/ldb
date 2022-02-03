package app;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import store.ldb.LDB;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LDBTest {
    private final File basedir = new File("tmp/ldb");
    private LDB store;

    @BeforeEach
    void setUp() throws IOException {
        if (basedir.exists()) FileUtils.deleteDirectory(basedir);
    }

    @Test
    public void testLevelZeroCompactionsHappenFromOldestToNewest() throws InterruptedException {
        store = new LDB(basedir.getPath(), 1, 1, 2);
        store.pauseCompactor();

        int value = 0;
        store.set("1", "a");
        store.set("1", "b");
        assertEquals("b", store.get("1").orElseThrow());
        assertFiles("wal2", "level0/seg0", "level0/seg1");

        store.runCompaction(0);
        assertEquals("b", store.get("1").orElseThrow());
        assertFiles("wal2", "level0/seg1", "level1/seg0");

        store.runCompaction(0);
        assertEquals("b", store.get("1").orElseThrow());
        assertFiles("wal2", "level1/seg1");
    }

    @Test
    public void testTwoLevelOverlappingCompactions() throws InterruptedException {
        store = new LDB(basedir.getPath(), 1, 1, 2);
        store.pauseCompactor();

        store.set("1", "a");
        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal1", "level0/seg0");

        store.set("2", "b");
        assertEquals("b", store.get("2").orElseThrow());
        assertFiles("wal2", "level0/seg0", "level0/seg1");

        store.set("1", "x");
        assertEquals("x", store.get("1").orElseThrow());
        assertEquals("b", store.get("2").orElseThrow());
        assertFiles("wal3", "level0/seg0", "level0/seg1", "level0/seg2");

        store.runCompaction(0);
        assertEquals("x", store.get("1").orElseThrow());
        assertEquals("b", store.get("2").orElseThrow());
        assertFiles("wal3", "level0/seg1", "level0/seg2", "level1/seg0");

        store.runCompaction(0);
        assertEquals("x", store.get("1").orElseThrow());
        assertEquals("b", store.get("2").orElseThrow());
        assertFiles("wal3", "level0/seg2", "level1/seg0", "level1/seg1");

        store.runCompaction(0);
        assertEquals("x", store.get("1").orElseThrow());
        assertEquals("b", store.get("2").orElseThrow());
        assertFiles("wal3", "level1/seg1", "level1/seg2");
    }

    @Test
    public void testThreeLevelOverlappingCompactions() throws InterruptedException {
        store = new LDB(basedir.getPath(), 1, 1, 3);
        store.pauseCompactor();

        store.set("1", "a");
        store.runCompaction(0);
        store.runCompaction(1);
        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal1", "level2/seg0");
    }

    private void assertFiles(String... filenames) {
        TreeSet<String> expected = new TreeSet<>(Arrays.asList(filenames));
        TreeSet<String> actual = FileUtils.listFiles(new File("tmp/ldb"), null, true).stream()
                .map(file -> file.getPath().replace("tmp/ldb/", ""))
                .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(expected, actual);
    }

}
