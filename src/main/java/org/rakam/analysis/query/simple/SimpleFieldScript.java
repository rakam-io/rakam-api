package org.rakam.analysis.query.simple;

import org.rakam.analysis.query.FieldScript;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 04/05/14.
 */
public class SimpleFieldScript<T> implements FieldScript<T> {
    private final String fieldKey;
    final transient String userData;

    public SimpleFieldScript(String fieldKey) {
        this.fieldKey = fieldKey;
        userData = fieldKey.startsWith("_user.") ? fieldKey.substring(6) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleFieldScript)) return false;

        SimpleFieldScript that = (SimpleFieldScript) o;

        if (!fieldKey.equals(that.fieldKey)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return fieldKey.hashCode();
    }

    @Override
    public boolean requiresUser() {
        return userData != null;
    }

    @Override
    public T extract(JsonObject event, JsonObject user) {
        return userData!=null ? user.getField(userData) : event.getField(fieldKey);
    }

    @Override
    public boolean contains(JsonObject event, JsonObject user) {
        return extract(event, user)!=null;
    }

    @Override
    public String toString() {
        return fieldKey;
    }
}
