package org.rakam.cache.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import org.joda.time.DateTime;
import org.rakam.cache.SimpleCacheAdapter;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.UUID;

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
    public void addToGroupByItem(UUID aggregation, String groupBy, String item) {
        IAtomicLong counter = hazelcast.getAtomicLong(aggregation.toString() + ":" + item);
        if (counter.getAndIncrement()==0) {
            hazelcast.getSet(aggregation.toString() + ":" + "::" + "keys").add(item);
        }
    }

    @Override
    public void addToGroupByItem(UUID aggregation, DateTime dateTime, String groupBy, String item) {
        long timestamp = dateTime.getMillis();
        IAtomicLong counter = hazelcast.getAtomicLong(aggregation.toString() + ":" + timestamp + ":" + item);
        if (counter.getAndIncrement()==0) {
            hazelcast.getSet(aggregation.toString() + ":" + timestamp + "::" + "keys").add(item);
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
    public JsonObject getMultiSetCounts(UUID id, List<Long> keys) {
        JsonObject counts = new JsonObject();
        for(Long key : keys) {
            ISet<String> map = hazelcast.getSet(id.toString() + ":" + key + "::keys");
            JsonObject keyObj = new JsonObject();
            counts.putObject(Long.toString(key), keyObj);

            for (String set: map) {
                long counter = hazelcast.getAtomicLong(id.toString() + ":" + key + ":" + set).get();
                keyObj.putNumber(set, counter);
            }
        }
        return counts;
    }

    @Override
    public JsonObject getMultiSetCounts(UUID id) {
        JsonObject counts = new JsonObject();
        ISet<String> map = hazelcast.getSet(id.toString() + "::keys");

        for (String set: map) {
            long counter = hazelcast.getAtomicLong(id.toString() + ":" + set).get();
            counts.putNumber(set, counter);
        }

        return counts;
    }

    @Override
    public JsonObject getMultiCounts(UUID id, List<Long> keys) {
        JsonObject counts = new JsonObject();
        for(Long key : keys) {
            IAtomicLong s = hazelcast.getAtomicLong(id.toString() + ":" + key);
            counts.putNumber(Long.toString(key), s == null ? 0 : s.get());
        }
        return counts;
    }

    @Override
    public void increment(UUID key, DateTime dateTime) {
        /* IMap<Long, IAtomicLong> map = hazelcast.getMap(key.toString());
        IAtomicLong a = map.get(dateTime.getMillis());
        if(a==null)
            a = .incrementAndGet(); */
        hazelcast.getAtomicLong(key.toString() + ":" + dateTime.getMillis()).incrementAndGet();;
    }

    @Override
    public void increment(UUID key) {
        hazelcast.getAtomicLong(key.toString()).incrementAndGet();
    }

    @Override
    public void addSet(String key, String value) {

    }

    @Override
    public int countSet(String key) {
        return 0;
    }

}
