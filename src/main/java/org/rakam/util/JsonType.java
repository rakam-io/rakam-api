package org.rakam.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/01/15 10:36.
 */
public enum JsonType {
    ARRAY("array"),
    BOOLEAN("boolean"),
    NULL("null"),
    NUMBER("number"),
    OBJECT("object"),
    STRING("string");

    private final String name;

    JsonType(String name) {
        this.name = name;
    }

    public static JsonType get(String name) {
        for (JsonType a : JsonType.values()) {
            if (a.name == name)
                return a;
        }
        throw new IllegalArgumentException("Invalid string");
    }

    @Override
    public String toString() {
        return name;
    }
}
