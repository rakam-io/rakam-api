package org.rakam.analysis.script;

import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;

/**
 * Created by buremba on 04/05/14.
 */
public abstract class FilterScript implements Serializable {
    public abstract boolean test(JsonObject obj);
    public abstract boolean hasUser();
    public abstract String toString();

}
