package org.rakam.constant;

/**
 * Created by buremba on 19/05/14.
 */
public enum AnalysisRuleStrategy {
    REAL_TIME_BATCH_CONCURRENT(1),
    REAL_TIME(2),
    BATCH(3),
    REAL_TIME_AFTER_BATCH(4);

    public final int id;
    AnalysisRuleStrategy(int id) {
        this.id = id;
    }

    public static AnalysisRuleStrategy get(int id){
        for (AnalysisRuleStrategy a: AnalysisRuleStrategy.values()) {
            if (a.id == id)
                return a;
        }
        throw new IllegalArgumentException("Invalid id");
    }

    public static AnalysisRuleStrategy get(String name) {
        if(name!=null)
            name = name.toUpperCase();
        return AnalysisRuleStrategy.valueOf(name);
    }
}
