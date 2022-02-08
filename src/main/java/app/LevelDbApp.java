package app;

import io.jooby.Context;
import io.jooby.Jooby;
import io.jooby.StatusCode;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class LevelDbApp extends Jooby {
    public static final Logger LOG = LoggerFactory.getLogger(LevelDbApp.class);
    private final DB store;

    {
        try {
            store = factory.open(new File("data/leveldb"), new Options());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        put("/probe/{probeId}/event/{eventId}", this::saveEvent);

        get("/probe/{probeId}/latest", this::getLatestEvent);

        //onStarted(() -> {
        //    LOG.info("stats: {}", store.stats());
        //});
    }

    private Object getLatestEvent(Context ctx) {
        final byte[] bytes = store.get(bytes(ctx.path().get("probeId").value()));
        if (bytes != null) {
            return new String(bytes);
        }
        ctx.setResponseCode(StatusCode.NOT_FOUND);
        return "";
    }

    private Object saveEvent(Context ctx) {
        String value = ctx.body().value();
        store.put(bytes(ctx.path().get("probeId").value()), bytes(value));
        return "saved";
    }

    public static void main(final String[] args) {
        runApp(args, LevelDbApp::new);
    }

}
