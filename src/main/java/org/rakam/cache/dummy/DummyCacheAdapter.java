package org.rakam.cache.dummy;

import org.rakam.cache.CacheAdapter;
import org.vertx.java.core.json.JsonObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by buremba on 14/06/14.
 */
public class DummyCacheAdapter implements CacheAdapter {
    @Override
    public JsonObject getActorProperties(String project, String actor_id) {
        return null;
    }

    @Override
    public void addActorProperties(String project, String actor_id, JsonObject properties) {

    }

    @Override
    public void setActorProperties(String project, String actor_id, JsonObject properties) {

    }

    @Override
    public void incrementCounter(String key, long increment) {

    }

    @Override
    public void setCounter(String s, long target) {

    }

    @Override
    public void addSet(String setName, String item) {

    }

    @Override
    public void removeSet(String setName) {

    }

    @Override
    public void removeCounter(String setName) {

    }

    @Override
    public Long getCounter(String key) {
        return null;
    }

    @Override
    public int getSetCount(String key) {
        return 0;
    }

    @Override
    public Iterator<String> getSetIterator(String key) {
        return null;
    }

    @Override
    public Set<String> getSet(String key) {
        return null;
    }

    @Override
    public void incrementCounter(String key) {

    }

    @Override
    public void addSet(String setName, Collection<String> items) {

    }

    @Override
    public void addGroupByItem(String aggregation, String groupBy, String item) {

    }

    @Override
    public void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy) {

    }

    @Override
    public void flush() {

    }
}
