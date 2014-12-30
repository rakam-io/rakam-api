package org.rakam.stream;

import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 23:53.
 */
public interface MetricGroupingStreamHandler {
    public void handleEvent(JsonObject event, JsonObject actor);
    public JsonElement get(int limit);
}
