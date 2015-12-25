package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;


public enum FieldType {
    STRING, DOUBLE, LONG, BOOLEAN, DATE, TIME, TIMESTAMP,
    ARRAY_STRING, ARRAY_DOUBLE, ARRAY_LONG, ARRAY_BOOLEAN, ARRAY_DATE, ARRAY_TIME, ARRAY_TIMESTAMP,
    MAP_STRING_STRING, MAP_STRING_DOUBLE;

    private static final FieldType values[] = values();

    @JsonCreator
    public static FieldType fromString(String key) {
        return key == null ? null : FieldType.valueOf(key.toUpperCase());
    }

    public boolean isArray() {
        return ordinal() > 6 && !isMap();
    }

    public boolean isMap() {
        return ordinal() > 13;
    }

    public FieldType getArrayElementType() {
        if(!isArray()) {
            throw new IllegalStateException("type is not array");
        }

        return values[ordinal() - 7];
    }

    public FieldType getMapValueType() {
        if(!isMap()) {
            throw new IllegalStateException("type is not map");
        }

        return values[ordinal() - 14];
    }

    public FieldType convertToMapValueType() {
        if(isMap()) {
            throw new IllegalStateException("type is already a map");
        }

        if(ordinal() > 3) {
            throw new IllegalStateException("map type is supported");
        }

        return values[ordinal() + 14];
    }

    public FieldType convertToArrayType() {
        if(ordinal() > 6) {
            throw new IllegalStateException("type is already array");
        }

        return values[ordinal() + 7];
    }
}
