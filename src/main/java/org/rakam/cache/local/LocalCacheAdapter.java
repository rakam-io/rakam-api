package org.rakam.cache.local;

import org.rakam.analysis.AverageCounter;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.hazelcast.hyperloglog.HLLWrapper;
import org.rakam.util.HLLWrapperImpl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by buremba on 21/05/14.
 */
public class LocalCacheAdapter implements CacheAdapter {
    protected final Map<String, AtomicLong> counters = new ConcurrentHashMap();
    protected final Map<String, Set<String>> sets = new ConcurrentHashMap();

    @Override
    public Long getCounter(String key) {
        AtomicLong a = counters.get(key);
        return (a == null) ? null : a.get();
    }


    @Override
    public int getSetCount(String key) {
        Set<String> a = sets.get(key);
        return (a == null) ? 0 : a.size();
    }

    @Override
    public Set<String> getSet(String key) {
        Set<String> strings = sets.get(key);
        if (strings == null) {
            HashSet<String> value = new HashSet<>();
            sets.put(key, value);
            return value;
        } else
            return strings;
    }

    @Override
    public void incrementCounter(String key) {
        incrementCounter(key, 1);
    }

    @Override
    public void incrementGroupBySimpleCounter(String aggregation, String groupBy, long incrementBy) {
        AtomicLong counter = counters.get(aggregation + ":" + groupBy);
        if (counter == null) {
            counters.put(aggregation + ":" + groupBy, new AtomicLong(incrementBy));
            addSet(aggregation + "::" + "keys", groupBy);
        } else {
            if (incrementBy == 1L)
                counter.incrementAndGet();
            else
                counter.addAndGet(incrementBy);
        }
    }

    @Override
    public void setGroupBySimpleCounter(String aggregation, String groupBy, long l) {
        AtomicLong counter = counters.get(aggregation + ":" + groupBy);
        if (counter == null) {
            counters.put(aggregation + ":" + groupBy, new AtomicLong(l));
            addSet(aggregation + "::" + "keys", groupBy);
        } else {
            counter.set(l);
        }
    }

    @Override
    public Long getGroupBySimpleCounter(String aggregation, String groupBy) {
        AtomicLong atomicLong = counters.get(aggregation + ":" + groupBy);
        return atomicLong == null ? null : atomicLong.get();
    }

    @Override
    public void incrementCounter(String key, long increment) {
        AtomicLong atomicLong = counters.get(key);
        if (atomicLong == null) {
            counters.put(key, new AtomicLong(increment));
        } else
            atomicLong.addAndGet(increment);
    }

    @Override
    public void setCounter(String s, long target) {
        AtomicLong atomicLong = counters.get(s);
        if (atomicLong == null)
            counters.put(s, new AtomicLong(target));
        else
            atomicLong.set(target);
    }

    @Override
    public void addSet(String setName, String item) {
        Set<String> s = sets.get(setName);
        if (s == null) {
            s = new ConcurrentSkipListSet();
            s.add(item);
            sets.put(setName, s);
        } else
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
        if (s == null) {
            s = new ConcurrentSkipListSet();
            s.addAll(items);
            sets.put(setName, s);
        } else
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
        for (String item : set) {
            removeCounter(key + ":" + item);
        }
        removeSet(key + "::keys");
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key) {
        Set<String> set = getSet(key + "::keys");
        Map<String, Set<String>> stringHashMap = new HashMap<>(set.size());

        for (String item : set) {
            stringHashMap.put(item, getSet(key + ":" + item));
        }
        return stringHashMap;
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key, int limit) {
        Set<String> set1 = getSet(key + "::keys");
        if(set1==null)
            return null;
        Iterator<String> set = set1.iterator();
        HashMap<String, Set<String>> stringHashMap = new HashMap<>(set1.size());

        int i = 0;
        while (i++ < limit && set.hasNext()) {
            String item = set.next();
            stringHashMap.put(item, getSet(key + ":" + item));
        }
        return stringHashMap;
    }

    @Override
    public Map<String, HLLWrapper> estimateGroupByStrings(String key, int limit) {
        Set<String> set1 = getSet(key + "::keys");
        if(set1==null)
            return null;
        Iterator<String> set = set1.iterator();
        HashMap<String, HLLWrapper> stringHashMap = new HashMap<>(set1.size());

        int i = 0;
        while (i++ < limit && set.hasNext()) {
            String item = set.next();
            HLLWrapperImpl hllWrapper = new HLLWrapperImpl();
            getSet(key + ":" + item).forEach(hllWrapper::add);

            stringHashMap.put(item, hllWrapper);
        }
        return stringHashMap;
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key, int limit) {
        Set<String> set = getSet(key + "::keys");
        if (set == null) return null;

        HashMap<String, Long> stringLongHashMap = new HashMap<>(set.size());

        Iterator<String> it = set.iterator();
        int i = 0;
        while (i++ < limit && it.hasNext()) {
            String item = it.next();
            stringLongHashMap.put(item, getCounter(key + ":" + item));
        }
        return stringLongHashMap;
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key) {
        return getGroupByCounters(key, Integer.MAX_VALUE);
    }

    @Override
    public void incrementGroupByAverageCounter(String id, String groupByValue, long sum, long counter) {
        incrementCounter(id + ":" + groupByValue + ":sum", sum);
        incrementCounter(id + ":" + groupByValue + ":count", counter);
        addSet(id + "::keys", groupByValue);
    }

    @Override
    public void incrementAverageCounter(String id, long sum, long counter) {
        AtomicLong atomicLong = counters.get(id + ":sum");
        if (atomicLong != null)
            atomicLong.addAndGet(sum);
        else
            counters.put(id + ":sum", new AtomicLong(sum));

        atomicLong = counters.get(id + ":count");
        if (atomicLong != null)
            atomicLong.addAndGet(counter);
        else
            counters.put(id + ":count", new AtomicLong(counter));
    }

    @Override
    public AverageCounter getAverageCounter(String id) {
        Long counter = getCounter(id + ":sum");
        Long counter1 = getCounter(id + ":count");
        return counter != null && counter1 != null ? new AverageCounter(counter, counter1) : null;
    }

    @Override
    public void removeGroupByStrings(String key) {
        Set<String> set = getSet(key + "::keys");
        for (String item : set) {
            removeSet(item + ":" + item);
        }
        removeSet(key + "::keys");
    }

    @Override
    public Map<String, Long> getGroupByStringsCounts(String key, Integer limit) {
        Set<String> set = getSet(key + "::keys");
        if(set==null)
            return null;
        Map<String, Long> stringHashMap = new HashMap<>(set.size());

        for (String item : set) {
            stringHashMap.put(item, (long) getSet(key + ":" + item).size());
        }
        return stringHashMap;
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String rule_id) {
        return getGroupByAverageCounters(rule_id, Integer.MAX_VALUE);
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String key, int limit) {
        Set<String> set = getSet(key + "::keys");
        if(set==null)
            return null;
        HashMap<String, AverageCounter> stringLongHashMap = new HashMap<>(set.size());
        if (set == null) return null;

        Iterator<String> it = set.iterator();
        int i = 0;
        while (i++ < limit && it.hasNext()) {
            String item = it.next();
            stringLongHashMap.put(item, getAverageCounter(key + ":" + item));
        }
        return stringLongHashMap;
    }

    @Override
    public HLLWrapper createHLLFromSets(String... keys) {
        HLLWrapperImpl hllWrapper = new HLLWrapperImpl();
        for (String key : keys) {
            getSet(key).forEach(hllWrapper::add);
        }
        return hllWrapper;
    }
}
