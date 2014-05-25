package org.rakam.cache;

import org.vertx.java.core.json.JsonObject;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by buremba on 21/12/13.
 */
public interface CacheAdapter {

    public abstract Long getCounter(String key);
    public abstract int getSetCount(String key);
    public abstract Iterator<String> getSetIterator(String key);
    public abstract Long incrementCounter(String key);
    public abstract void addGroupByItem(String aggregation, String groupBy, String item);
    public abstract void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy);
    public abstract JsonObject getActorProperties(String project, String actor_id);
    public abstract void addActorProperties(String project, String actor_id, JsonObject properties);
    public abstract Long incrementCounter(String key, long increment);
    public abstract void setCounter(String s, long target);
    public abstract void addToSet(String setName, String item);

    public abstract void addToSet(String setName, Collection<String> items);

    public abstract void setActorProperties(String project, String actor_id, JsonObject properties);
    public abstract void flush();
}
