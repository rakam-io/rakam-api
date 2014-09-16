package org.rakam.analysis;

import org.rakam.cache.CacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/09/14 15:51.
 */
public class EventFilter {
    private final DatabaseAdapter databaseAdapter;
    private final CacheAdapter cacheAdapter;

    public EventFilter(CacheAdapter cacheAdapter, DatabaseAdapter databaseAdapter) {
        this.databaseAdapter = databaseAdapter;
        this.cacheAdapter = cacheAdapter;
    }

    public JsonObject handle(JsonObject query) {
        return null;
    }
}
