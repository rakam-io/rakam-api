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
    public abstract String extract(JsonObject event, JsonObject actor_data);
    public abstract boolean contains(JsonObject obj);
    public abstract String toString();
    public abstract boolean requiresUser();
}
