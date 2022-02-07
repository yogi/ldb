package app;

import io.jooby.StatusCode;
import kotlin.ranges.IntRange;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import store.ldb.LDB;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegrationTest {

    static OkHttpClient client = new OkHttpClient();

    @Test()
    public void testMultipleSetsAndGetsWithCompactorOn() throws InterruptedException, IOException {
        File basedir = new File("tmp/ldb");
        if (basedir.exists()) FileUtils.deleteDirectory(basedir);
        LDB store = new LDB(basedir.getPath());

        BlockingQueue<String> queue = new LinkedBlockingDeque<>();
        new Thread(() -> {
            final int max = 10000;
            final String randomString = RandomStringUtils.randomAlphabetic(5 * 1024);
            for (int i = 0; i < max; i++) {
                final String key = String.valueOf(i);
                store.set(key, i + "_" + randomString);
                queue.add(key);
            }
        }).start();

        do {
            String key = queue.take();
            assertTrue(store.get(key).orElseThrow(() -> new AssertionError(format("key %s not found", key))).startsWith(key + "_"));
        } while (!queue.isEmpty());
    }

    @Test
    public void testGets() {
        int expectedEntries = 3000000;

        new IntRange(1, expectedEntries).forEach(n -> {
            try {
                String probeId = "PRB" + n;
                Request req = new Request.Builder()
                        .url("http://localhost:8080/probe/" + probeId + "/latest")
                        .build();
                try (Response rsp = client.newCall(req).execute()) {
                    String msg = "failed for probe: " + probeId;
                    boolean check = false;
                    if (check) {
                        assertEquals(StatusCode.OK.value(), rsp.code(), msg);
                        assertTrue(rsp.body().string().contains("7707d6a0-61b5-11ec-9f10-0800200c9a66" + n), msg);
                    } else {
                        if (StatusCode.OK.value() != rsp.code()) {
                            System.out.println(msg);
                        }
                    }
                }
                if (n % 1000 == 0) {
                    System.out.println(n);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
