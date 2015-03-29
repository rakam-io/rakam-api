package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by buremba on 19/05/14.
 */
public enum TableStrategy {
    STREAM,
    INCREMENTAL;

    @JsonCreator
    public static TableStrategy get(String name) {
        return valueOf(name.toUpperCase());
    }

    @JsonProperty
    public String value() {
        return name();
    }
}
