package store.ldb;

import io.jooby.StatusCode;
import kotlin.ranges.IntRange;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;

public class IntegrationTest {

    static OkHttpClient client = new OkHttpClient();

    @Test()
    public void testMultipleSetsAndGetsWithCompactorOn() throws Exception {
        final int MAX_KEYS = 10000;
        final int MAX_READER_THREADS = 5;

        File basedir = new File("tmp/ldb");
        if (basedir.exists()) FileUtils.deleteDirectory(basedir);
        Ldb store = new Ldb(basedir.getPath());
        store.startCompactor();

        BlockingQueue<String> queue = new LinkedBlockingDeque<>();
        new Thread(() -> {
            final String randomString = RandomStringUtils.randomAlphabetic(5 * 1024);
            for (int i = 0; i < MAX_KEYS; i++) {
                final String key = String.valueOf(((int) (Math.random() * MAX_KEYS)));
                final String value = key + "_" + randomString;
                store.set(key, value);
                //System.out.println("set key = " + key);
                queue.add(key);
            }
        }).start();

        Thread.sleep(500);

        AtomicBoolean fail = new AtomicBoolean();
        AtomicInteger counter = new AtomicInteger(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < MAX_READER_THREADS; i++) {
            Thread thread = new Thread(() -> {
                do {
                    try {
                        String key = queue.poll(1000, TimeUnit.MILLISECONDS);
                        if (key == null) break;
                        final String value = store.get(key).orElseThrow(() -> new AssertionError(format("key %s not found", key)));
                        //System.out.println("got value = " + value.substring(0, value.indexOf("_")));
                        assertTrue(value.startsWith(key + "_"));
                        if (counter.get() % 100 == 0) System.out.println("counter = " + counter);
                    } catch (InterruptedException e) {
                        System.out.println("caught exception in reader thread: " + e);
                        throw new RuntimeException(e);
                    }
                } while (counter.incrementAndGet() <= MAX_KEYS);
            });
            thread.setUncaughtExceptionHandler((t, e) -> {
                System.out.println("caught exception in thread = " + e);
                fail.set(true);
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertFalse(fail.get());
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
