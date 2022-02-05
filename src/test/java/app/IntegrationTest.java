package app;

import io.jooby.StatusCode;
import kotlin.ranges.IntRange;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegrationTest {

    static OkHttpClient client = new OkHttpClient();

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
                    assertEquals(StatusCode.OK.value(), rsp.code(), msg);
                    assertTrue(rsp.body().string().contains("7707d6a0-61b5-11ec-9f10-0800200c9a66" + n), msg);
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
