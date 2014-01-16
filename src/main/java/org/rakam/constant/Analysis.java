package org.rakam.constant;


import java.io.Serializable;

/**
 * Created by buremba on 21/12/13.
 */

public enum Analysis implements Serializable {
    ANALYSIS_METRIC(0),
    ANALYSIS_TIMESERIES(1);

    public final int id;
    Analysis(int id) {
        this.id = id;
    }

    public static Analysis get(int id){
        for (Analysis a: Analysis.values()) {
            if (a.id == id)
                return a;
        }
        throw new IllegalArgumentException("Invalid id");
    }
}
