package org.rakam.stream;

import org.rakam.stream.kume.TimeSeriesStreamHandler;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/07/14 06:38.
 */
public class AverageCounter implements Counter {
    long count;
    long sum;

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
        this.sum += sum;
        this.count += count;
    }

    public void increment(long sum) {
        this.sum += sum;
        this.count++;
    }

    public static AverageCounter merge(AverageCounter val0, AverageCounter val1) {
        return val0.getCount() > val1.getCount() ? val0 : val1;
    }
}
