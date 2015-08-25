package org.rakam.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 23:36.
 */
public enum AggregationType {
    COUNT,
    COUNT_UNIQUE,
    SUM,
    MINIMUM,
    MAXIMUM,
    APPROXIMATE_UNIQUE,
    VARIANCE,
    POPULATION_VARIANCE,
    STANDARD_DEVIATION,
    AVERAGE;

    @JsonCreator
    public static AggregationType get(String name) {
        return valueOf(name.toUpperCase());
    }

    @JsonProperty
    public String value() {
        return name();
    }
}
