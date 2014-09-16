package org.rakam.analysis.query;

import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;

/**
 * Created by buremba on 04/05/14.
 */
public interface FieldScript<T> extends Serializable {
    public abstract T extract(JsonObject event, JsonObject user);
    public abstract boolean contains(JsonObject obj, JsonObject user);
    public abstract boolean requiresUser();
}
