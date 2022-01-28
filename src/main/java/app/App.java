package app;

import io.jooby.Context;
import io.jooby.Jooby;
import io.jooby.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class App extends Jooby {

    public static final Logger LOG = LoggerFactory.getLogger(App.class);
    private final Store store = new Store("data");

    {
        put("/probe/{probeId}/event/{eventId}", this::saveEvent);

        get("/probe/{probeId}/latest", this::getLatestEvent);

        get("/stats", this::stats);
    }

    private Object stats(Context context) {
        Map<String, Object> stats = new HashMap<>();
        stats.put("memtable-entries", store.memtable.size());
        return stats.toString();
    }

    private Object getLatestEvent(Context ctx) {
        String event = store.get(ctx.path().get("probeId").value());
        if (event == null) {
            ctx.setResponseCode(StatusCode.NOT_FOUND);
            return "";
        } else {
            return event;
        }
    }

    private Object saveEvent(Context ctx) {
        String value = ctx.body().value();
        store.set(ctx.path().get("probeId").value(), value);
        return "saved";
    }

    public static void main(final String[] args) {
        runApp(args, App::new);
    }

}
