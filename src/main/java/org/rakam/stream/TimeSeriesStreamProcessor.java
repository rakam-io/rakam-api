package org.rakam.stream;

import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 31/12/14 02:25.
 */
public interface TimeSeriesStreamProcessor {
    public void handleEvent(JsonObject event, JsonObject actor);
    public JsonElement get(int timestamp, int limit);
}
