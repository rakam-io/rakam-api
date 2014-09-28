package org.rakam.constant;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 22:56.
 */
public enum AggregationAnalysis {
    COUNT(0),
    COUNT_X(1),
    COUNT_UNIQUE_X(2),
    SUM_X(3),
    MINIMUM_X(4),
    MAXIMUM_X(5),
    AVERAGE_X(6),
    SELECT_UNIQUE_X(7);

    public final int id;
    AggregationAnalysis(int id) {
        this.id = id;
    }

    public static AggregationAnalysis get(int id){
        for (AggregationAnalysis a: AggregationAnalysis.values()) {
            if (a.id == id)
                return a;
        }
        throw new IllegalArgumentException("Invalid id");
    }

    public static AggregationAnalysis get(String name){
        name = name.toUpperCase();
        return AggregationAnalysis.valueOf(name);
    }

    public AggregationType[] getAnalyzableAggregationTypes() {
        switch (this) {
            case SELECT_UNIQUE_X:
            case COUNT_UNIQUE_X:
                return new AggregationType[]{AggregationType.UNIQUE_X};
            case COUNT_X:
                return new AggregationType[]{AggregationType.COUNT_X, AggregationType.AVERAGE_X};
            case SUM_X:
                return new AggregationType[]{AggregationType.SUM_X, AggregationType.AVERAGE_X};
            default:
                return new AggregationType[]{AggregationType.get(this.id)};
        }
    }
}
