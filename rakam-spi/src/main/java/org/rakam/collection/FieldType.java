package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;


public enum FieldType {
    STRING, LONG, DOUBLE, BOOLEAN, DATE, TIME, TIMESTAMP,
    ARRAY_STRING, ARRAY_LONG, ARRAY_DOUBLE, ARRAY_BOOLEAN, ARRAY_DATE, ARRAY_TIME, ARRAY_TIMESTAMP;

    private static final FieldType values[] = values();

    @JsonCreator
    public static FieldType fromString(String key) {
        return key == null ? null : FieldType.valueOf(key.toUpperCase());
    }

    public boolean isArray() {
        return ordinal() > 6;
    }

    public FieldType getArrayType() {
        if(ordinal() < 7) {
            throw new IllegalStateException("type is not array");
        }

        return values[ordinal() - 7];
    }

    public FieldType convertToArrayType() {
        if(ordinal() > 6) {
            throw new IllegalStateException("type is already array");
        }

        return values[ordinal() + 7];
    }
}
