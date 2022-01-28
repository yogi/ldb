package app;

import kotlin.ranges.IntRange;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

public class MapPerf {
    private static final String KEY_SUFFIX = genString(100);
    private static final String VAL_SUFFIX = genString(5000);

    @Test
    public void testCreation() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        new IntRange(1, (int) Math.pow(10, 4)).forEach(i -> map.put(i + KEY_SUFFIX, i + VAL_SUFFIX));
        long start = System.currentTimeMillis();
        ConcurrentHashMap<String, String> newMap = new ConcurrentHashMap<>(map);
        long end = System.currentTimeMillis();
        System.out.println("newMap " + newMap.size() + " took " + (end - start) + " ms");
    }

    private static String genString(Integer len) {
        StringBuffer buf = new StringBuffer();
        new IntRange(1, len).forEach(i -> buf.append("k"));
        return buf.toString();
    }
}
