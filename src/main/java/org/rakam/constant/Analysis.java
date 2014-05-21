package org.rakam.constant;


/**
 * Created by buremba on 21/12/13.
 */

public enum Analysis {
    ANALYSIS_METRIC(0),
    ANALYSIS_TIMESERIES(1),
    ANALYSIS_GAUGE(2);

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

    public static Analysis get(String name) {
        if(name!=null)
            name = name.toUpperCase();
        return Analysis.valueOf("ANALYSIS_"+name);
    }
}
