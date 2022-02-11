package app;

import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import store.ldb.CompressionType;
import store.ldb.Config;
import store.ldb.Ldb;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LdbTest {
    private final File basedir = new File("tmp/ldb");
    private Ldb store;
    private Config.ConfigBuilder defaultConfig;

    @BeforeEach
    void setUp() throws IOException {
        deleteDataDir();
        defaultConfig = new Config.ConfigBuilder()
                .withCompressionType(CompressionType.LZ4)
                .withLevelCompactionThreshold((level) -> 1)
                .withNumLevels(1)
                .withMaxBlockSize(100)
                .withMaxWalSize(1);
    }

    @AfterEach
    void tearDown() {
        if (store != null) store.stop();
    }

    @Test
    public void testLZ4() throws UnsupportedEncodingException {
        String s = RandomStringUtils.randomAlphabetic(1000);
        byte[] data = s.getBytes("UTF-8");
        final int origLength = data.length;

        final byte[] compressed = CompressionType.LZ4.compress(s.getBytes());
        final byte[] uncompressed = CompressionType.LZ4.uncompress(compressed);

        assertEquals(s, new String(uncompressed));
    }

    @Test
    public void testMultipleSetsAndGets() {
        final Config config = defaultConfig.withNumLevels(3).build();
        store = new Ldb(basedir.getPath(), config);

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
        store = new Ldb(basedir.getPath(), defaultConfig.build());

        store.set("1", "a");
        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal1", "level0/seg0");

        store = new Ldb(basedir.getPath(), defaultConfig.build());

        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal2", "level0/seg0");

        store.set("1", "b");
        assertEquals("b", store.get("1").orElseThrow());
        assertFiles("wal3", "level0/seg0", "level0/seg1");
    }

    @Test
    public void testPromoteSegment() {
        final Config config = defaultConfig
                .withLevelCompactionThreshold((level) -> 1)
                .withNumLevels(2)
                .withMaxBlockSize(1)
                .build();
        store = new Ldb(basedir.getPath(), config);

        store.set("1", "a");
        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal1", "level0/seg0");

        store.runCompaction(0);
        assertEquals("a", store.get("1").orElseThrow());
        assertFiles("wal1", "level1/seg0"); // no compactions below limit of 4 for level-0
    }

    @Test
    public void testLevelZeroCompactionsHappenFromOldestToNewest() {
        final Config config = defaultConfig
                .withLevelCompactionThreshold((level) -> 4)
                .withNumLevels(2)
                .withMaxBlockSize(1)
                .build();
        store = new Ldb(basedir.getPath(), config);

        store.set("1", "a");
        store.set("1", "b");
        assertEquals("b", store.get("1").orElseThrow());
        assertFiles("wal2", "level0/seg0", "level0/seg1");

        store.runCompaction(0);
        assertEquals("b", store.get("1").orElseThrow());
        assertFiles("wal2", "level0/seg0", "level0/seg1"); // no compactions below limit of 4 for level-0

        store.set("1", "c");
        store.set("1", "d");
        store.set("1", "e");
        store.set("1", "f");
        assertEquals("f", store.get("1").orElseThrow());  // all level-0 ones should be picked for compactions
        assertFiles("wal6", "level0/seg0", "level0/seg1", "level0/seg2", "level0/seg3", "level0/seg4", "level0/seg5"); // no compactions below limit of 4 for level-0

        store.runCompaction(0);
        assertFiles("wal6", "level1/seg0");
    }

    @Test
    public void testThreeLevelOverlappingCompactions() {
        final Config config = defaultConfig
                .withNumLevels(3)
                .build();
        store = new Ldb(basedir.getPath(), config);

        store.set("1", "a");
        store.set("1", "b");
        store.set("1", "c");
        store.set("1", "d");
        store.runCompaction(0);
        assertEquals("d", store.get("1").orElseThrow());
        store.runCompaction(1);
        assertEquals("d", store.get("1").orElseThrow());
        assertFiles("wal4", "level2/seg0");
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
