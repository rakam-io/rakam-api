package org.rakam.cache.local;

import org.rakam.cache.CacheAdapter;
import org.vertx.java.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by buremba on 21/05/14.
 */
public class LocalCacheAdapter implements CacheAdapter {
    static Map<String, AtomicLong> counters = new ConcurrentHashMap();
    static Map<String, Set<String>> sets = new ConcurrentHashMap();

    @Override
    public Long getCounter(String key) {
        AtomicLong a = counters.get(key);
        return (a==null) ? 0 : a.get();
    }


    @Override
    public int getSetCount(String key) {
        Set<String> a = sets.get(key);
        return (a==null) ? 0 : a.size();
    }

    @Override
    public Iterator<String> getSetIterator(String key) {
        Set<String> s = sets.get(key);
        return (s==null) ? null : s.iterator();
    }

    @Override
    public Long incrementCounter(String key) {
        AtomicLong a = counters.get(key);
        if(a==null) {
            counters.put(key, new AtomicLong(1));
            return 1L;
        }else {
            return a.incrementAndGet();
        }
    }

    @Override
    public void addGroupByItem(String aggregation, String groupBy, String item) {
        AtomicLong counter = counters.get(aggregation + ":" + item);
        if (counter.getAndIncrement()==0) {
            sets.get(aggregation + "::" + "keys").add(item);
        }
    }

    @Override
    public void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy) {
        AtomicLong counter = counters.get(aggregation + ":" + item);
        if (counter.getAndAdd(incrementBy)==0) {
            sets.get(aggregation + "::" + "keys").add(item);
        }
    }

    @Override
    public JsonObject getActorProperties(String project, String actor_id) {
        return null;
    }

    @Override
    public void addActorProperties(String project, String actor_id, JsonObject properties) {
    }

    @Override
    public Long incrementCounter(String key, long increment) {
        return counters.get(key).incrementAndGet();
    }

    @Override
    public void setCounter(String s, long target) {
        counters.get(s).set(target);
    }

    @Override
    public void addToSet(String setName, String item) {
        Set<String> s = sets.get(setName);
        if(s==null) {
            s = new ConcurrentSkipListSet();
            s.add(item);
            sets.put(setName, s);
        }else
            s.add(item);
    }

    @Override
    public void addToSet(String setName, Collection<String> items) {
        Set<String> s = sets.get(setName);
        if(s==null) {
            s = new ConcurrentSkipListSet();
            s.addAll(items);
            sets.put(setName, s);
        }else
            s.addAll(items);
    }

    @Override
    public void setActorProperties(String project, String actor_id, JsonObject properties) {

    }

    public void flush() {
        sets.clear();
        counters.clear();
    }
}
