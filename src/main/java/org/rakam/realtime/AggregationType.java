package org.rakam.realtime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 23:36.
 */
public enum AggregationType {
    COUNT("count"),
    COUNT_UNIQUE("count_unique"),
    SUM("sum"),
    MINIMUM("minimum"),
    MAXIMUM("maximum"),
    APPROXIMATE_UNIQUE("approximate_unique"),
    APPROXIMATE_PERCENTILE("approximate_percentile"),
    VARIANCE("variance"),
    POPULATION_VARIANCE("population_variance"),
    STANDARD_DEVIATION("standard_deviation"),
    POPULATION_STANDARD_DEVIATION("population_standard_deviation"),
    AVERAGE("average");

    public final String value;

    AggregationType(String id) {
        this.value = id;
    }

    @JsonProperty
    public String value() {
        return value;
    }
}
