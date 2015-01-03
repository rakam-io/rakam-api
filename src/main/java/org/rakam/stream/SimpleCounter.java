package org.rakam.stream;


/**
 * Created by buremba <Burak Emre Kabakcı> on 29/12/14 03:22.
 */
public class SimpleCounter implements Counter {
    long counter;

    public SimpleCounter() {
    }

    public SimpleCounter(long counter) {
        this.counter = counter;
    }

    @Override
    public long getValue() {
        return counter;
    }

    public void increment() {
        counter++;
    }

    public void add(long l) {
        counter += l;
    }

    public void set(long l) {
        counter = l;
    }

    public static long merge(long val0, long val1) {
        return Long.max(val0, val1);
    }

    public static SimpleCounter merge(SimpleCounter val0, SimpleCounter val1) {
        return val0.getValue() > val1.getValue() ? val0 : val1;
    }
}
