package org.rakam.stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 23:36.
 */
public enum AggregationType {
    COUNT(0),
    COUNT_X(1),
    UNIQUE_X(2),
    SUM_X(3),
    MINIMUM_X(4),
    MAXIMUM_X(5),
    AVERAGE_X(6);

    public final int id;

    AggregationType(int id) {
        this.id = id;
    }

    public static AggregationType get(int id) {
        for (AggregationType a : AggregationType.values()) {
            if (a.id == id)
                return a;
        }
        throw new IllegalArgumentException("Invalid id");
    }

    public static AggregationType get(String name) {
        if (name != null)
            name = name.toUpperCase();
        return AggregationType.valueOf(name);
    }
}
