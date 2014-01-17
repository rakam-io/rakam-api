package org.rakam.cache;

import org.joda.time.DateTime;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public abstract class SimpleCacheAdapter {

    public abstract String get(String key); // TODO: return type?

    public abstract Long getCounter(String key);

    public abstract void set(String key, String value);

    public abstract JsonObject getMultiSetCounts(UUID id);

    public abstract JsonObject  getMultiCounts(UUID agg_rule, List<Long> keys);
    public abstract JsonObject  getMultiSetCounts(UUID agg_rule, List<Long> keys);

    public abstract void increment(UUID key, DateTime dateTime);
    public abstract void increment(UUID key);

    public abstract void addSet(String key, String value);

    public abstract int countSet(String key);

    public SimpleCacheAdapter() {
        //
    };

    public abstract void addToGroupByItem(UUID aggregation, String groupBy, String item);

    public abstract void addToGroupByItem(UUID aggregation, DateTime dateTime, String groupBy, String item);

    public abstract JsonObject getActorProperties(String project, String actor_id);

    public abstract void addActorProperties(String project, String actor_id, JsonObject properties);
}
