package app;

import io.jooby.Context;
import io.jooby.Jooby;
import io.jooby.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.ldb.LDB;

import java.util.Optional;

public class App extends Jooby {
    public static final Logger LOG = LoggerFactory.getLogger(App.class);
    private final LDB store;

    {
        try {
            store = new LDB("data/ldb");
            store.startCompactor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        put("/probe/{probeId}/event/{eventId}", this::saveEvent);

        get("/probe/{probeId}/latest", this::getLatestEvent);

        get("/stats", this::stats);

        onStarted(() -> {
            LOG.info("stats: {}", store.stats());
        });
    }

    private Object stats(Context context) {
        return store.stats();
    }

    private Object getLatestEvent(Context ctx) {
        Optional<String> event = store.get(ctx.path().get("probeId").value());
        if (event.isPresent()) {
            return event.get();
        }
        ctx.setResponseCode(StatusCode.NOT_FOUND);
        return "";
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
