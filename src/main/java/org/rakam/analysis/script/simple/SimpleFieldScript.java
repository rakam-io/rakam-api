package org.rakam.analysis.script.simple;

import org.rakam.analysis.script.FieldScript;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 04/05/14.
 */
public class SimpleFieldScript extends FieldScript {
    final String userData;
    public SimpleFieldScript(String fieldKey) {
        super(fieldKey);
        userData = fieldKey.startsWith("_user.") ? fieldKey.substring(6) : null;
    }

    @Override
    public boolean requiresUser() {
        return userData != null;
    }

    @Override
    public String extract(JsonObject event, JsonObject user_data) {
        return userData!=null && user_data!=null ? user_data.getString(userData) : event.getString(fieldKey);
    }

    @Override
    public boolean contains(JsonObject obj) {
        return obj.getField(fieldKey)!=null;
    }

    @Override
    public String toString() {
        return fieldKey;
    }
}
