package app;

import io.jooby.Context;
import io.jooby.Jooby;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App extends Jooby {

    public static final Logger LOG = LoggerFactory.getLogger(App.class);
    private Store store = new Store("data");

    {
        put("/probe/{probeId}/event/{eventId}", ctx -> saveEvent(ctx));

        get("/probe/{probeId}/latest", ctx -> getLatestEvent(ctx));
    }

    private Object getLatestEvent(Context ctx) {
        String event = store.get(ctx.path().get("probeId").value());
        return event == null ? "" : event;
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
