package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/03/15 03:46.
 */
public enum FieldType {
    STRING, ARRAY, LONG, DOUBLE, BOOLEAN, DATE, HYPERLOGLOG, TIME, TIMESTAMP;

    @JsonCreator
    public static FieldType fromString(String key) {
        return key == null ? null : FieldType.valueOf(key.toUpperCase());
    }
}
