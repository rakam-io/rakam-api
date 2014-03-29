package org.rakam.cache.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import org.rakam.cache.SimpleCacheAdapter;
import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;
import java.util.List;

/**
 * Created by buremba on 21/12/13.
 */

public class HazelcastCacheAdapter extends SimpleCacheAdapter {
    private final HazelcastInstance hazelcast;

    public HazelcastCacheAdapter() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("analytics").setPassword("");
        hazelcast =  HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Override
    public void addGroupByItem(String aggregation, String groupBy, String item) {
        IAtomicLong counter = hazelcast.getAtomicLong(aggregation + ":" + item);
        if (counter.getAndIncrement()==0) {
            hazelcast.getSet(aggregation + "::" + "keys").add(item);
        }
    }

    @Override
    public void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy) {
        IAtomicLong counter = hazelcast.getAtomicLong(aggregation + ":" + item);
        if (counter.getAndAdd(incrementBy)==0) {
            hazelcast.getSet(aggregation + "::" + "keys").add(item);
        }
    }

    @Override
    public JsonObject getActorProperties(String project, String actor_id) {
        IMap<String, JsonObject> map = hazelcast.getMap(project.toString()+":actor-prop");
        return map.get(actor_id);
    }

    @Override
    public void addActorProperties(String project, String actor_id, JsonObject properties) {
        IMap<String, JsonObject> map = hazelcast.getMap(project.toString()+":actor-prop");
        map.put(actor_id, properties);
    }

    @Override
    public Long incrementCounter(String key, long increment) {
        return hazelcast.getAtomicLong(key).getAndAdd(increment);
    }

    @Override
    public void setCounter(String key, long target) {
        hazelcast.getAtomicLong(key).set(target);
    }

    @Override
    public void addToSet(String setName, String item) {
        hazelcast.getSet(setName).add(item);
    }

    @Override
    public String get(String key) {
        return null;
    }


    @Override
    public Long getCounter(String key) {
        return hazelcast.getAtomicLong(key).get();

    }

    @Override
    public void set(String key, String value) {

    }

    @Override
    public JsonObject getMultiSetCounts(String id, List<Long> keys) {
        JsonObject counts = new JsonObject();
        for(Long key : keys) {
            ISet<String> map = hazelcast.getSet(id + ":" + key + ":keys");
            JsonObject keyObj = new JsonObject();
            counts.putObject(Long.toString(key), keyObj);

            for (String set: map) {
                long counter = hazelcast.getAtomicLong(id + ":" + key + ":" + set).get();
                keyObj.putNumber(set, counter);
            }
        }
        return counts;
    }

    @Override
    public int getSetCount(String key) {
       return hazelcast.getSet(key).size();
    }

    @Override
    public Iterator getSetIterator(String key) {
        return hazelcast.getSet(key).iterator();
    }

    @Override
    public JsonObject getMultiCounts(String id, List<Long> keys) {
        JsonObject counts = new JsonObject();
        for(Long key : keys) {
            IAtomicLong s = hazelcast.getAtomicLong(id + ":" + key);
            counts.putNumber(Long.toString(key), s == null ? 0 : s.get());
        }
        return counts;
    }

    @Override
    public Long incrementCounter(String key) {
        return hazelcast.getAtomicLong(key.toString()).incrementAndGet();
    }

}
