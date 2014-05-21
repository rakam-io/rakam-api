package org.rakam.analysis.script;

import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;

/**
 * Created by buremba on 04/05/14.
 */
public abstract class FieldScript implements Serializable {
    public final String fieldKey;
    public FieldScript(String fieldKey) {
        this.fieldKey = fieldKey;
    }
    public abstract String extract(JsonObject obj);
    public abstract boolean contains(JsonObject obj);
    public abstract String toString();
}
