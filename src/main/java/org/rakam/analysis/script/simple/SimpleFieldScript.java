package org.rakam.analysis.script.simple;

import org.rakam.analysis.script.FieldScript;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 04/05/14.
 */
public class SimpleFieldScript extends FieldScript {
    public SimpleFieldScript(String fieldKey) {
        super(fieldKey);
    }

    @Override
    public String extract(JsonObject obj) {
        return obj.getString(fieldKey);
    }

    @Override
    public boolean contains(JsonObject obj) {
        return obj.containsField(fieldKey);
    }

    @Override
    public String toString() {
        return fieldKey;
    }
}
