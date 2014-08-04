package org.rakam.cache.local;

import org.apache.commons.lang.NotImplementedException;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.hazelcast.models.AverageCounter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by buremba on 21/05/14.
 */
public class LocalCacheAdapter implements CacheAdapter {
    public final static boolean ORDERED = false;

    final static Map<String, AtomicLong> counters = new ConcurrentHashMap();
    final static Map<String, Set<String>> sets = new ConcurrentHashMap();

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
    public Set<String> getSet(String key) {
        Set<String> strings = sets.get(key);
        if(strings==null)  {
            HashSet<String> value = new HashSet<>();
            sets.put(key, value);
            return value;
        }else
            return strings;
    }

    @Override
    public void incrementCounter(String key) {
        incrementCounter(key, 1);
    }

    @Override
    public boolean isOrdered() {
        return ORDERED;
    }

    @Override
    public void addGroupByCounter(String aggregation, String groupBy) {
        addGroupByCounter(aggregation, groupBy, 1L);
    }

    @Override
    public void addGroupByCounter(String aggregation, String groupBy, long incrementBy) {
        AtomicLong counter = counters.get(aggregation + ":" + groupBy);
        if(counter==null) {
            counters.put(aggregation + ":" + groupBy, new AtomicLong(incrementBy));
            addSet(aggregation + "::" + "keys", groupBy);
        }else {
            if(incrementBy==1L)
                counter.incrementAndGet();
            else
                counter.addAndGet(incrementBy);
        }
    }

    @Override
    public void incrementCounter(String key, long increment) {
        AtomicLong atomicLong = counters.get(key);
        if (atomicLong == null) {
            counters.put(key, new AtomicLong(increment));
        }else
            atomicLong.addAndGet(increment);
    }

    @Override
    public void setCounter(String s, long target) {
        counters.get(s).set(target);
    }

    @Override
    public void addSet(String setName, String item) {
        Set<String> s = sets.get(setName);
        if(s==null) {
            s = new ConcurrentSkipListSet();
            s.add(item);
            sets.put(setName, s);
        }else
            s.add(item);
    }

    @Override
    public void removeSet(String setName) {
        sets.remove(setName);
    }

    @Override
    public void removeCounter(String counterName) {
        counters.remove(counterName);
    }

    @Override
    public void addSet(String setName, Collection<String> items) {
        Set<String> s = sets.get(setName);
        if(s==null) {
            s = new ConcurrentSkipListSet();
            s.addAll(items);
            sets.put(setName, s);
        }else
            s.addAll(items);
    }

    public void flush() {
        sets.clear();
        counters.clear();
    }

    @Override
    public void addGroupByString(String id, String groupByValue, String type_target) {
        addSet(id + ":" + groupByValue, type_target);
        addSet(id + "::keys", groupByValue);
    }

    @Override
    public void addGroupByString(String id, String groupByValue, Collection<String> s) {
        addSet(id + ":" + groupByValue, s);
        addSet(id + "::keys", groupByValue);
    }

    @Override
    public void removeGroupByCounters(String key) {
        Set<String> set = getSet(key + "::keys");
        for(String item : set) {
            removeCounter(key + ":" + item);
        }
        removeSet(key + "::keys");
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key) {
        Set<String> set = getSet(key + "::keys");
        HashMap<String, Set<String>> stringHashMap = new HashMap<>(set.size());

        for(String item : set) {
            stringHashMap.put(item, getSet(key+":"+item));
        }
        return stringHashMap;
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key, int limit) {
        Set<String> set1 = getSet(key + "::keys");
        Iterator<String> set = set1.iterator();
        HashMap<String, Set<String>> stringHashMap = new HashMap<>(set1.size());

        int i = 0;
        while(i++<limit && set.hasNext()) {
            String item = set.next();
            stringHashMap.put(item, getSet(item+":"+item));
        }
        return stringHashMap;
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key, int limit) {
        Set<String> set = getSet(key + "::keys");
        HashMap<String, Long> stringLongHashMap = new HashMap<>(set.size());
        if(set==null) return null;

        Iterator<String> it = set.iterator();
        int i = 0;
        while(i++<limit && it.hasNext()) {
            String item = it.next();
            stringLongHashMap.put(item, getCounter(key + ":" + item).longValue());
        }
        return stringLongHashMap;
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key) {
        return getGroupByCounters(key, Integer.MAX_VALUE);
    }

    @Override
    public void incrementGroupByAverageCounter(String id, String groupByValue, long sum, long counter) {
        incrementCounter(id + ":sum:" + groupByValue, sum);
        incrementCounter(id + ":count:" + groupByValue, counter);
        addSet(id, groupByValue);
    }

    @Override
    public void incrementAverageCounter(String id, long sum, long counter) {
        AtomicLong atomicLong = counters.get(id + ":sum");
        if(atomicLong!=null)
            atomicLong.addAndGet(sum);
        else
            counters.put(id + ":sum", new AtomicLong(sum));

        atomicLong = counters.get(id + ":count");
        if(atomicLong!=null)
            atomicLong.addAndGet(counter);
        else
            counters.put(id + ":count", new AtomicLong(counter));
    }

    @Override
    public AverageCounter getAverageCounter(String id) {
        return new AverageCounter(getCounter(id+":sum"), getCounter(id+":count"));
    }

    @Override
    public void removeGroupByStrings(String key) {
        Set<String> set = getSet(key + "::keys");
        for(String item : set) {
            removeSet(item + ":" + item);
        }
        removeSet(key + "::keys");
    }

    @Override
    public Map<String, Long> getGroupByStringsCounts(String key, Integer limit) {
        throw new NotImplementedException();
    }
}
