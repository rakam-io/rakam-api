package org.rakam.stream;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/07/14 06:38.
 */
public class AverageCounter implements Counter {
    private static final AtomicLongFieldUpdater<AverageCounter> ATOMIC_UPDATER_COUNT =
            AtomicLongFieldUpdater.newUpdater(AverageCounter.class, "count");
    private static final AtomicLongFieldUpdater<AverageCounter> ATOMIC_UPDATER_SUM =
            AtomicLongFieldUpdater.newUpdater(AverageCounter.class, "sum");

    protected volatile long count;
    protected volatile long sum;

    public AverageCounter(long sum, long count) {
        this.count = count;
        this.sum = sum;
    }
    public AverageCounter() {
    }

    public long getValue() {
        return count == 0 ? 0 : sum / count;
    }

    public long getSum() {
        return sum;
    }


    public long getCount() {
        return count;
    }

    public void add(long sum, long count) {
        ATOMIC_UPDATER_COUNT.addAndGet(this, count);
        ATOMIC_UPDATER_SUM.addAndGet(this, sum);
    }
}
