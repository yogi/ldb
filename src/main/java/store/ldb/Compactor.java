package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Compactor {
    public static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
    public static final int SLEEP_BETWEEN_COMPACTIONS_MS = 10;
    private final TreeMap<Integer, Level> levels;
    private final Thread thread;
    private final AtomicBoolean stop;

    public Compactor(TreeMap<Integer, Level> levels) {
        this.levels = levels;
        this.stop = new AtomicBoolean(false);
        this.thread = new Thread(this::compact);
    }

    private void compact() {
        LOG.info("started compaction loop");
        while(!stop.get()) {
            for (Level level : levels.values()) {
                level.compact();
                sleepSilently();
            }
        }
        LOG.info("compaction loop stopped");
    }

    private void sleepSilently() {
        try {
            Thread.sleep(SLEEP_BETWEEN_COMPACTIONS_MS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static Compactor start(TreeMap<Integer, Level> levels) {
        final Compactor compactor = new Compactor(levels);
        compactor.start();
        return compactor;
    }

    private void start() {
        thread.start();
    }

    public void stop() {
        stop.set(true);
    }
}
