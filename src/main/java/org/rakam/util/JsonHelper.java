package org.rakam.util;

import org.vertx.java.core.MultiMap;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;

/**
 * Created by buremba on 17/01/14.
 */
public class JsonHelper {
    public static JsonObject generate(MultiMap map) {
        JsonObject obj = new JsonObject();
        for (Map.Entry<String, String> item : map) {
            String key = item.getKey();
            obj.putString(key, item.getValue());
        }
        return obj;
    }
    public static JsonObject generate(Map<String, String> map) {
        JsonObject obj = new JsonObject();
        for (Map.Entry<String, String> item : map.entrySet()) {
            String key = item.getKey();
            obj.putString(key, item.getValue());
        }
        return obj;
    }

    public static JsonObject returnError(String message) {
        JsonObject j = new JsonObject();
        j.putString("error", message);
        return j;
    }
}
