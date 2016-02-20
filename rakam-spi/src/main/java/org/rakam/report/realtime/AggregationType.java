package org.rakam.report.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public enum AggregationType {
    COUNT,
    COUNT_UNIQUE,
    SUM,
    MINIMUM,
    MAXIMUM,
    AVERAGE,
    APPROXIMATE_UNIQUE;

    @JsonCreator
    public static AggregationType get(String name) {
        return valueOf(name.toUpperCase());
    }

    @JsonProperty
    public String value() {
        return name();
    }
}
