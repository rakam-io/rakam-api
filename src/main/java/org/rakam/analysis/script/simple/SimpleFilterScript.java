package org.rakam.analysis.script.simple;

import org.rakam.analysis.script.FilterScript;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;
import java.util.Set;

/**
 * Created by buremba on 05/05/14.
 */
public class SimpleFilterScript extends FilterScript {

    public Set<Map.Entry<String, Object>> fields;

    public SimpleFilterScript(Map<String, Object> fields) {
        this.fields = fields.entrySet();
    }

    @Override
    public boolean test(JsonObject obj) {
        for(Map.Entry<String, Object> field : fields)
            if(!obj.getObject(field.getKey()).equals(field.getValue()))
                return false;
        return true;
    }

    @Override
    public boolean hasUser() {
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
