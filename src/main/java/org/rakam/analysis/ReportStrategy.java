package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by buremba on 19/05/14.
 */
public enum ReportStrategy {
    REAL_TIME,
    UPDATE_PERIODICALLY,
    UPDATE_ON_DEMAND,
    BATCH;

    @JsonCreator
    public static ReportStrategy get(String name) {
        return valueOf(name.toUpperCase());
    }

    @JsonProperty
    public String value() {
        return name();
    }
}
