package org.rakam.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 31/12/14 01:33.
 */
public class LongArrayMap<V> extends NumberArrayMap<Long, V> {

    private static final long serialVersionUID = -2304239764179127L;

    public LongArrayMap(int minBound, int maxBound) {
        super(minBound, maxBound);
    }

    @Override
    protected Long makeKeyFromInt(int k) {
        return (long) k;
    }

}