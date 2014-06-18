package org.rakam.cache.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.*;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.MessageListener;
import org.rakam.cache.PubSubAdapter;
import org.vertx.java.core.json.JsonObject;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by buremba on 21/12/13.
 */

public class HazelcastCacheAdapter implements CacheAdapter, PubSubAdapter {
    private static HazelcastInstance hazelcast;


    public HazelcastCacheAdapter() throws FileNotFoundException {
        ClientConfig clientConfig = new ClientConfig();
        FileSystemXmlConfig config = new FileSystemXmlConfig("config/cluster.xml");
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
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
        IMap<String, JsonObject> map = hazelcast.getMap(project + ":actor-prop");
        return map.get(actor_id);
    }

    @Override
    public void addActorProperties(String project, String actor_id, JsonObject properties) {
        IMap<String, JsonObject> map = hazelcast.getMap(project+":actor-prop");
        map.put(actor_id, properties);
    }

    @Override
    public void incrementCounter(String key, long increment) {
        hazelcast.getAtomicLong(key).addAndGet(increment);
    }

    @Override
    public void setCounter(String key, long target) {
        hazelcast.getAtomicLong(key).set(target);
    }

    @Override
    public void addSet(String setName, String item) {
        hazelcast.getSet(setName).add(item);
    }

    @Override
    public void removeSet(String setName) {
        hazelcast.getSet(setName).clear();
    }

    @Override
    public void removeCounter(String setName) {
        hazelcast.getAtomicLong(setName).set(0);
    }

    @Override
    public void addSet(String setName, Collection<String> items) {
        if(items!=null)
            hazelcast.getSet(setName).addAll(items);
    }

    @Override
    public void setActorProperties(String project, String actor_id, JsonObject properties) {
        hazelcast.getMap(project+":actor-prop").set(actor_id, properties);
    }

    @Override
    public void flush() {

    }

    @Override
    public Long getCounter(String key) {
        return hazelcast.getAtomicLong(key).get();

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
    public Set<String> getSet(String key) {
        return hazelcast.getSet(key);
    }

    @Override
    public void incrementCounter(String key) {
        hazelcast.getAtomicLong(key.toString()).incrementAndGet();
        return;
    }


    @Override
    public String subscribe(String id, final MessageListener run) {
        ITopic topic = hazelcast.getTopic(id);
        return topic.addMessageListener(new com.hazelcast.core.MessageListener() {
            @Override
            public void onMessage(Message message) {
                run.onMessage(message);
            }
        });
    }

    @Override
    public void publish(String id, String message) {
        hazelcast.getTopic(id).publish(message);
    }

    @Override
    public void desubscribe(String id, String subscription_id) {
        hazelcast.getTopic(id).removeMessageListener(subscription_id);
    }
}
