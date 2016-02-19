package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;


public enum FieldType {
    STRING, DOUBLE, LONG, BOOLEAN, DATE, TIME, TIMESTAMP, BINARY,
    ARRAY_STRING, ARRAY_DOUBLE, ARRAY_LONG, ARRAY_BOOLEAN, ARRAY_DATE, ARRAY_TIME, ARRAY_TIMESTAMP, ARRAY_BINARY,
    MAP_STRING, MAP_DOUBLE, MAP_LONG, MAP_BOOLEAN, MAP_DATE, MAP_TIME, MAP_TIMESTAMP, MAP_BINARY;

    private static final FieldType values[] = values();

    @JsonCreator
    public static FieldType fromString(String key) {
        return key == null ? null : FieldType.valueOf(key.toUpperCase());
    }

    public boolean isArray() {
        return ordinal() > 7 && !isMap();
    }

    public boolean isMap() {
        return ordinal() > 15;
    }

    public FieldType getArrayElementType() {
        if(!isArray()) {
            throw new IllegalStateException("type is not array");
        }

        return values[ordinal() - 8];
    }

    public FieldType getMapValueType() {
        if(!isMap()) {
            throw new IllegalStateException("type is not map");
        }

        return values[ordinal() - 16];
    }

    public FieldType convertToMapValueType() {
        if(isMap()) {
            throw new IllegalStateException("type is already a map");
        }
        if(isArray()) {
            throw new IllegalStateException("type is already a array");
        }

        return values[ordinal() + 16];
    }

    public FieldType convertToArrayType() {
        if(ordinal() > 7) {
            throw new IllegalStateException("type is already array");
        }

        return values[ordinal() + 8];
    }

    public String getPrettyName() {
        if (isArray()) {
            return "ARRAY<"+getArrayElementType().toString()+">";
        }
        if(isMap()) {
            return "MAP<STRING, "+getMapValueType().toString()+">";
        }
        return toString();
    }
}
