package store.ldb;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class Throttler {
    public static final int MIN_SLEEP_NANOS = 10000;
    public static final int SLEEP_INCREMENT_NANOS = 10000;
    public static final int MAX_SLEEP_NANOS = 1000000;
    public static final int ONE_MILLI_IN_NANOS = 1000000;
    final AtomicBoolean throttling = new AtomicBoolean();
    final AtomicInteger sleepDurationNanos = new AtomicInteger(MIN_SLEEP_NANOS);
    private final Supplier<Boolean> thresholdCheck;

    public Throttler(Config config, Supplier<Boolean> thresholdCheck) {
        this.thresholdCheck = thresholdCheck;
        if (config.enableThrottling) {
            new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(this::checkThreshold, 0, 1, TimeUnit.SECONDS);
        }
    }

    void checkThreshold() {
        boolean breached = thresholdCheck.get();
        if (!breached && throttling.get()) {
            if (sleepDurationNanos.get() > MIN_SLEEP_NANOS) {
                sleepDurationNanos.set(sleepDurationNanos.get() - SLEEP_INCREMENT_NANOS);
                Ldb.LOG.info("decrease throttling sleep duration to {} ms", sleepDurationInMillis());
            } else {
                Ldb.LOG.info("stop throttling");
                throttling.set(false);
                sleepDurationNanos.set(MIN_SLEEP_NANOS);
            }
        } else if (breached) {
            if (throttling.get()) {
                sleepDurationNanos.set(Math.min(sleepDurationNanos.get() + SLEEP_INCREMENT_NANOS, MAX_SLEEP_NANOS));
                Ldb.LOG.info("increase throttling sleep duration to {} ms", sleepDurationInMillis());
            } else {
                Ldb.LOG.info("start throttling, sleep duration is {} ms", sleepDurationInMillis());
                throttling.set(true);
            }
        }
    }

    private double sleepDurationInMillis() {
        return sleepDurationNanos.get() / (double) ONE_MILLI_IN_NANOS;
    }

    public void throttle() {
        if (throttling.get()) {
            try {
                final int nanos = sleepDurationNanos.get();
                Thread.sleep(nanos / ONE_MILLI_IN_NANOS, nanos % ONE_MILLI_IN_NANOS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
