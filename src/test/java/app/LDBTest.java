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

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LDBTest {
    private final File basedir = new File("tmp/ldb");
    private LDB store;

    @BeforeEach
    void setUp() throws IOException {
        deleteDataDir();
    }

    @Test()
    public void testx() throws InterruptedException {
        final byte l = (byte) 'L';
        final byte d = (byte) 'D';
        final byte b = (byte) 'B';
        final byte e = (byte) '!';
        int marker = (l << 24) | (d << 16) | (b << 8) | e;

        System.out.println("marker = " + marker);

        System.out.println(Integer.toBinaryString(l));
        System.out.println(Integer.toBinaryString(d));
        System.out.println(Integer.toBinaryString(b));
        System.out.println(Integer.toBinaryString(e));
        System.out.println(Integer.toBinaryString(marker));

        System.out.println(Integer.toHexString(l));
        System.out.println(Integer.toHexString(d));
        System.out.println(Integer.toHexString(b));
        System.out.println(Integer.toHexString(e));

    }

    @Test()
    public void testMultipleSetsAndGetsWithCompactorOn() throws InterruptedException {
        final int maxBlockSize = 10; // force multiple blocks to be written per segment
        final int maxSegmentSize = 20;

        store = new LDB(basedir.getPath(), maxSegmentSize, 1, 1, maxBlockSize);
        store.pauseCompactor();

        final int max = 10;
        for (int i = 0; i < max; i++) {
            store.set(String.valueOf(i), String.valueOf(i));
        }
        for (int i = 0; i < max; i++) {
            final String s = String.valueOf(i);
            String msg = format("key %s not found", s);
            assertEquals(s, store.get(s).orElseThrow(() -> new AssertionError(msg)));
        }
    }

    @Test
    public void testMultipleSetsAndGets() {
        store = new LDB(basedir.getPath(), 1, 1, 3, 100);
        store.pauseCompactor();

        final int max = 10;
        for (int i = 0; i < max; i++) {
            store.set(String.valueOf(i), String.valueOf(i));
        }
        for (int i = 0; i < max; i++) {
            assertEquals(String.valueOf(i), store.get(String.valueOf(i)).orElseThrow());
        }

        for (int i = 0; i < max; i++) {
            store.runCompaction(0);
            for (int j = 0; j < max; j++) {
                assertEquals(String.valueOf(j), store.get(String.valueOf(j)).orElseThrow());
            }
        }

        for (int i = 0; i < max; i++) {
            store.runCompaction(1);
            for (int j = 0; j < max; j++) {
                assertEquals(String.valueOf(j), store.get(String.valueOf(j)).orElseThrow());
            }
        }
    }

    @Test
    public void testBlockStorage() {
        store = new LDB(basedir.getPath(), 1, 1, 1, 100);
        store.pauseCompactor();

        store.set("1", "a");
        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal1", "level0/seg0");

        store = new LDB(basedir.getPath(), 1, 1, 1, 100);
        store.pauseCompactor();

        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal2", "level0/seg0");

        store.set("1", "b");
        assertEquals("b", store.get("1").orElseThrow());
        assertFiles("wal3", "level0/seg0", "level0/seg1");
    }

    @Test
    public void testLevelZeroCompactionsHappenFromOldestToNewest() {
        store = new LDB(basedir.getPath(), 1, 1, 2, 100);
        store.pauseCompactor();

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
    public void testTwoLevelOverlappingCompactions() {
        store = new LDB(basedir.getPath(), 1, 1, 2, 100);
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
        store = new LDB(basedir.getPath(), 1, 1, 3, 100);
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

    private void deleteDataDir() throws IOException {
        if (basedir.exists()) FileUtils.deleteDirectory(basedir);
    }

}
