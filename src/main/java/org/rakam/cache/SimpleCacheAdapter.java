package org.rakam.cache;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;
import java.util.List;

/**
 * Created by buremba on 21/12/13.
 */
public abstract class SimpleCacheAdapter {

    public abstract String get(String key); // TODO: return type?

    public abstract Long getCounter(String key);

    public abstract void set(String key, String value);

    public abstract JsonObject  getMultiCounts(String agg_rule, List<Long> keys);
    public abstract JsonObject  getMultiSetCounts(String agg_rule, List<Long> keys);

    public abstract int getSetCount(String key);
    public abstract Iterator getSetIterator(String key);

    public abstract Long incrementCounter(String key);

    public SimpleCacheAdapter() {};

    public abstract void addGroupByItem(String aggregation, String groupBy, String item);

    public abstract void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy);

    public abstract JsonObject getActorProperties(String project, String actor_id);

    public abstract void addActorProperties(String project, String actor_id, JsonObject properties);

    public abstract Long incrementCounter(String key, long increment);

    public abstract void setCounter(String s, long target);

    public abstract void addToSet(String setName, String item);
}
