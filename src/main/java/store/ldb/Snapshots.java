package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Snapshots {
    public static final Logger LOG = LoggerFactory.getLogger(Snapshots.class);

    private final AtomicReference<Snapshot> current;

    public Snapshots(Snapshot initialSnapshot) {
        current = new AtomicReference<>(initialSnapshot);
    }

    public Snapshot current() {
        return current.get();
    }

    public void updateCurrent(List<Segment> segmentsCreated, List<Segment> segmentsDeleted) {
        while(true) {
            Snapshot prev = current.get();
            final Snapshot next = prev.createNextSnapshot(segmentsCreated, segmentsDeleted);
            if (current.compareAndSet(prev, next)) {
                return;
            } else {
                LOG.info("current snapshot was concurrently updated, retrying");
            }
        }
    }
}
