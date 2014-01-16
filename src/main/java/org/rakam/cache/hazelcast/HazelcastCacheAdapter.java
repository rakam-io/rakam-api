package org.rakam.cache.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;
import org.joda.time.DateTime;
import org.rakam.cache.SimpleCacheAdapter;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        IMap<String, ISet> map = hazelcast.getMap(aggregation.toString());
        map.get(groupBy).add(item);
    }

    @Override
    public void addToGroupByItem(UUID aggregation, DateTime dateTime, String groupBy, String item) {
        long timestamp = dateTime.getMillis();
        IMap<Long, IMap<String, ISet>> map = hazelcast.getMap(aggregation.toString());
        map.get(timestamp).get(groupBy).add(item);
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
    public Map<Long, Integer> getMultiCounts(UUID id, List<Long> keys) {
        IMap<Long, ISet> map = hazelcast.getMap(id.toString());
        Map<Long, Integer> counts = new HashMap();
        for(Long key : keys) {
            counts.put(key, map.get(key).size());
        }
        return counts;
    }

    @Override
    public void increment(UUID key, DateTime dateTime) {
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
