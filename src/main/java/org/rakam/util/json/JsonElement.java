package org.rakam.util.json;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 12:51.
 */

import java.io.Serializable;

public abstract class JsonElement implements Serializable {
    public static final JsonElement TRUE = new JsonElement() {
    };
    public static final JsonElement FALSE = new JsonElement() {
    };
    public static final JsonElement NULL = new JsonElement() {
    };

    JsonElement() {
    }

    public static JsonElement valueOf(int value) {
        return new JsonInteger(value);
    }

    public static JsonElement valueOf(long value) {
        return new JsonLong(value);
    }

    public static JsonElement valueOf(float value) {
        if (Float.isInfinite(value) || Float.isNaN(value)) {
            throw new IllegalArgumentException("Infinite and NaN values not permitted in JSON");
        }
        return new JsonFloat(value);
    }

    public static JsonElement valueOf(String string) {
        return string == null ? NULL : new JsonString(string);
    }

    public static JsonElement valueOf(boolean value) {
        return value ? TRUE : FALSE;
    }

    public boolean isObject() {
        return false;
    }

    public boolean isArray() {
        return false;
    }

    public boolean isNumber() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public static class JsonLong extends JsonElement {
        long value;

        public JsonLong(long value) {
            this.value = value;
        }

        @Override
        public boolean isNumber() {
            return true;
        }
    }

    public static class JsonInteger extends JsonElement {
        int value;

        public JsonInteger(int value) {
            this.value = value;
        }

        @Override
        public boolean isNumber() {
            return true;
        }
    }

    public static class JsonFloat extends JsonElement {
        float value;

        public JsonFloat(float value) {
            this.value = value;
        }

        @Override
        public boolean isNumber() {
            return true;
        }
    }

    public static class JsonString extends JsonElement {
        String value;

        public JsonString(String value) {
            this.value = value;
        }

        @Override
        public boolean isNumber() {
            return true;
        }
    }

}
