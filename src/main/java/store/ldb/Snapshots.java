package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class Snapshots {
    public static final Logger LOG = LoggerFactory.getLogger(Snapshots.class);

    private final AtomicReference<Snapshot> current;

    public Snapshots(Snapshot initialSnapshot) {
        requireNonNull(initialSnapshot, "null initial snapshot");
        current = new AtomicReference<>(initialSnapshot);
        initialSnapshot.acquire();
    }

    public Snapshot acquireCurrent() {
        final Snapshot snapshot = current.get();
        snapshot.acquire();
        return snapshot;
    }

    public void updateCurrent(List<Segment> segmentsCreated, List<Segment> segmentsDeleted) {
        while(true) {
            Snapshot prev = current.get();
            final Snapshot next = prev.createNextSnapshot(segmentsCreated, segmentsDeleted);
            if (current.compareAndSet(prev, next)) {
                prev.deleteSegmentsOnFinalization(segmentsDeleted);
                prev.release();
                next.acquire();
                return;
            } else {
                LOG.info("current snapshot was concurrently updated, retrying");
            }
        }
    }
}
