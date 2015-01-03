package org.rakam.analysis.query;

import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.io.Serializable;

/**
 * Created by buremba on 04/05/14.
 */
public interface FilterScript extends Serializable {
    public abstract boolean test(JsonObject event);

    public abstract boolean requiresUser();

    public abstract JsonElement toJson();
}