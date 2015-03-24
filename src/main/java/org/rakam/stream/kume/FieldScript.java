package org.rakam.stream.kume;

import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.io.Serializable;

/**
 * Created by buremba on 04/05/14.
 */
public interface FieldScript<T> extends Serializable {
    public abstract T extract(JsonObject event, JsonObject user);

    public abstract boolean contains(JsonObject obj, JsonObject user);

    public abstract boolean requiresUser();

    JsonElement toJson();
}
