package org.rakam.stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 05/01/15 01:57.
 */

/**
 * Useful when the sum may overflow in AverageCounter.
 */
public class IterativeMeanCounter implements Counter {
    long avg;
    float weight;

    public IterativeMeanCounter(long avg, long weight) {
        this.avg = avg;
        this.weight = weight;
    }

    public IterativeMeanCounter() {
        weight = 1;
    }

    public long getValue() {
        return avg;
    }


    public void increment(long sum) {
        avg += (sum - avg) / weight;
        ++weight;
    }

}
