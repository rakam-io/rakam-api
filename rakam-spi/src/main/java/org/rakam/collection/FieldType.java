package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;


public enum FieldType {
    STRING, ARRAY, LONG, DOUBLE, BOOLEAN, DATE, TIME, TIMESTAMP;

    @JsonCreator
    public static FieldType fromString(String key) {
        return key == null ? null : FieldType.valueOf(key.toUpperCase());
    }
}
