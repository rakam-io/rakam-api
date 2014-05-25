package org.rakam.analysis.script.simple;

import org.rakam.analysis.script.FilterScript;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;
import java.util.Set;

/**
 * Created by buremba on 05/05/14.
 */
public class SimpleFilterScript extends FilterScript {
    final boolean requiresUser;
    public Set<Map.Entry<String, Object>> fields;

    public SimpleFilterScript(Map<String, Object> fields) {
        this.fields = fields.entrySet();
        requiresUser = _requiresUser();
    }

    @Override
    public boolean test(JsonObject obj, JsonObject user) {
        for(Map.Entry<String, Object> field : fields)
            if((user!=null && field.getKey().startsWith("_user.") && !user.getObject(field.getKey().substring(6)).equals(field.getValue())) ||
                    !obj.getObject(field.getKey()).equals(field.getValue()))
                return false;
        return true;
    }

    @Override
    public boolean requiresUser() {
        return requiresUser;
    }

    boolean _requiresUser() {
        for(Map.Entry<String, Object> field : fields)
            if(field.getKey().startsWith("_user."))
                return true;
        return false;
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}
