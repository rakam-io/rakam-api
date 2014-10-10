package org.rakam.cache.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.PagingPredicate;
import org.rakam.analysis.AverageCounter;
import org.rakam.cache.ActorCacheAdapter;
import org.rakam.cache.DistributedCacheAdapter;
import org.rakam.cache.MessageListener;
import org.rakam.cache.PubSubAdapter;
import org.rakam.cache.hazelcast.hyperloglog.HLLWrapper;
import org.rakam.cache.hazelcast.models.Counter;
import org.rakam.cache.hazelcast.models.SimpleCounter;
import org.rakam.cache.hazelcast.operations.AddAllStringSetOperation;
import org.rakam.cache.hazelcast.operations.AddStringSetOperation;
import org.rakam.cache.hazelcast.operations.AverageCounterIncrementOperation;
import org.rakam.cache.hazelcast.operations.CounterIncrementOperation;
import org.rakam.cache.hazelcast.operations.CounterSetOperation;
import org.rakam.cache.hazelcast.operations.GetStringCountsOperation;
import org.rakam.util.HLLWrapperImpl;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by buremba on 21/12/13.
 */

public class HazelcastCacheAdapter implements DistributedCacheAdapter, ActorCacheAdapter, PubSubAdapter {
    private static HazelcastInstance hazelcast;

    public HazelcastCacheAdapter() throws IOException {
        ClientConfig clientConfig = new XmlClientConfigBuilder("config/hazelcast-client.xml").build();
        hazelcast =  HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Override
    public JsonObject getActorProperties(String project, String actor_id) {
        IMap<String, JsonObject> map = hazelcast.getMap(project + ":actor-prop");
        return map.get(actor_id);
    }

    @Override
    public void addActorProperties(String project, String actor_id, JsonObject properties) {
        IMap<String, JsonObject> map = hazelcast.getMap(project+":actor-prop");
        map.set(actor_id, properties);
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
        for(DistributedObject obj : hazelcast.getDistributedObjects())
            obj.destroy();
    }

    @Override
    public void addGroupByString(String id, String groupByValue, String type_target) {
        /*addSet(id + ":" + groupByValue, type_target);
        addSet(id + "::keys", groupByValue);
*/
        IMap<String, Set<String>> map = hazelcast.getMap(id);
        map.executeOnKey(groupByValue, new AddStringSetOperation(type_target));
    }

    @Override
    public void addGroupByString(String id, String groupByValue, Collection<String> s) {
        IMap<String, Set<String>> map = hazelcast.getMap(id);
        if (s.size()>0)
            map.executeOnKey(groupByValue, new AddAllStringSetOperation(s));
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
    public Set<String> getSet(String key) {
        return hazelcast.getSet(key);
    }

    @Override
    public void incrementCounter(String key) {
        hazelcast.getAtomicLong(key.toString()).incrementAndGet();
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


    @Override
    public void incrementGroupBySimpleCounter(String id, String key, long counter) {
        IMap<String, SimpleCounter> map = hazelcast.getMap(id);
        map.executeOnKey(key, new CounterIncrementOperation(counter));
    }

    @Override
    public void setGroupBySimpleCounter(String aggregation, String groupBy, long l) {
        IMap<String, SimpleCounter> map = hazelcast.getMap(aggregation);
        map.executeOnKey(groupBy, new CounterSetOperation(l));
    }

    @Override
    public Long getGroupBySimpleCounter(String aggregation, String groupBy) {
        IMap<String, SimpleCounter> map = hazelcast.getMap(aggregation);
        return map.get(groupBy).getValue();
    }


    @Override
    public Map<String, Long> getGroupByCounters(String id) {
        return getGroupByCounters(id, Integer.MAX_VALUE);
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key) {
        return getGroupByStrings(key, Integer.MAX_VALUE);
    }


    @Override
    public Map<String, Set<String>> getGroupByStrings(String key, int limit) {
        IMap<String, Set<String>> map = hazelcast.getMap(key);
        Map.Entry<String, Set<String>>[] values = (Map.Entry<String, Set<String>>[]) map.entrySet(new LimitPredicate("this", limit)).toArray(new Map.Entry[0]);
        Arrays.sort(values, (o1, o2) -> o2.getValue().size() - o1.getValue().size());
        LinkedHashMap<String, Set<String>> stringLongLinkedHashMap = new LinkedHashMap<>(Math.min(limit, values.length));
        for(int i=0; i<values.length; i++) {
            stringLongLinkedHashMap.put(values[i].getKey(), values[i].getValue());
        }
        return stringLongLinkedHashMap;
    }

    @Override
    public Map<String, HLLWrapper> estimateGroupByStrings(String key, int limit) {
        IMap<String, Set<String>> map = hazelcast.getMap(key);
        final LinkedHashMap<String, HLLWrapper> hashMap = new LinkedHashMap<>();
        map.executeOnEntries(new EntryProcessor<String, Set<String>>() {
            @Override
            public HLLWrapper process(Map.Entry<String, Set<String>> entry) {
                final HLLWrapperImpl hllWrapper = new HLLWrapperImpl();
                entry.getValue().forEach(hllWrapper::add);
                return hllWrapper;
            }

            @Override
            public EntryBackupProcessor getBackupProcessor() {
                return null;
            }
        }, new PagingPredicate(limit)).entrySet().stream()
                .sorted(Comparator.comparing(e -> ((HLLWrapper) e.getValue()).cardinality()))
                .forEach(e->hashMap.put(e.getKey(), (HLLWrapper) e.getValue()));

        return hashMap;
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key, int limit) {
        IMap<String, Counter> map = hazelcast.getMap(key);
        Map.Entry<String, Counter>[] values = map.entrySet(new LimitPredicate("this", limit)).toArray(new Map.Entry[0]);
        Arrays.sort(values, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        LinkedHashMap<String, Long> stringLongLinkedHashMap = new LinkedHashMap<>(Math.min(limit, values.length));
        for(int i=0; i<values.length && i<limit; i++) {
            stringLongLinkedHashMap.put(values[i].getKey(), values[i].getValue().getValue());
        }
        return stringLongLinkedHashMap;
    }

    @Override
    public void removeGroupByCounters(String id) {
        hazelcast.getMap(id).flush();
    }

    @Override
    public void incrementGroupByAverageCounter(String id, String key, long sum, long counter) {
        IMap<Object, Object> map = hazelcast.getMap(id);
        map.executeOnKey(key, new AverageCounterIncrementOperation(sum, counter));
    }

    @Override
    public void incrementAverageCounter(String id, long sum, long counter) {
        hazelcast.getAtomicLong(id + ":sum").addAndGet(sum);
        hazelcast.getAtomicLong(id+":count").addAndGet(counter);
    }

    @Override
    public AverageCounter getAverageCounter(String id) {
        return new AverageCounter(getCounter(id+":sum"), getCounter(id + ":count"));
    }

    @Override
    public void removeGroupByStrings(String key) {
        hazelcast.getMap(key).destroy();
    }

    @Override
    public Map<String, Long> getGroupByStringsCounts(String key, Integer limit) {
        IMap<String, Set<String>> map = hazelcast.getMap(key);
        Map.Entry<String, Integer>[] aThis = map.executeOnEntries(new GetStringCountsOperation(limit), new LimitPredicate("this", Integer.MAX_VALUE)).entrySet().toArray(new Map.Entry[0]);
        Arrays.sort(aThis, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        LinkedHashMap<String, Long> stringLongLinkedHashMap = new LinkedHashMap<>(limit);

        for(int i=0; i<limit && i<aThis.length; i++) {
            Map.Entry<String, Integer> aThi = aThis[i];
            stringLongLinkedHashMap.put(aThi.getKey(), aThi.getValue().longValue());
        }
        return stringLongLinkedHashMap;
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String key) {
         return getGroupByAverageCounters(key, Integer.MAX_VALUE);
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String key, int limit) {
        IMap<String, AverageCounter> map = hazelcast.getMap(key);
        Map.Entry<String, AverageCounter>[] values = map.entrySet(new LimitPredicate("this", limit)).toArray(new Map.Entry[0]);
        Arrays.sort(values, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        LinkedHashMap<String, AverageCounter> stringLongLinkedHashMap = new LinkedHashMap<>(Math.min(limit, values.length));
        for(int i=0; i<values.length && i<limit; i++) {
            stringLongLinkedHashMap.put(values[i].getKey(), values[i].getValue());
        }
        return stringLongLinkedHashMap;
    }

    @Override
    public HLLWrapper createHLLFromSets(String... keys) {
        // TODO: change to parallel computing
        HLLWrapperImpl hllWrapper = new HLLWrapperImpl();
        for (String key : keys) {
            getSet(key).forEach(hllWrapper::add);
        }
        return hllWrapper;
    }

}
