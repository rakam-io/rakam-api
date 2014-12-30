package org.rakam.stream.local;

import org.rakam.stream.AverageCounter;
import org.rakam.stream.Counter;
import org.rakam.stream.SimpleCounter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by buremba on 21/05/14.
 */
public class LocalCacheImpl implements LocalCache {
    protected final Map<String, AtomicLong> counters = new ConcurrentHashMap();
    protected final Map<String, Map<String, SimpleCounter>> groupByDefaultCounters = new ConcurrentHashMap<>();
    protected final Map<String, Map<String, AverageCounter>> groupByAverageCounters = new ConcurrentHashMap<>();
    protected final Map<String, Map<String, Set<String>>> groupBySets = new ConcurrentHashMap();
    protected final Map<String, Set<String>> sets = new ConcurrentHashMap();

    public LocalCacheImpl() {
    }

    @Override
    public void incrementGroupByCounter(String id, String groupByValue, long incrementBy) {
        createOrGetGroupByDefaultCounter(id, groupByValue).add(incrementBy);
    }

    @Override
    public void addGroupByString(String id, String groupByValue, String item) {
        getInternalGroupBySet(id, groupByValue).add(item);
    }

    @Override
    public Long getGroupByCounter(String id, String groupByValue) {
        Map<String, SimpleCounter> set = groupByDefaultCounters.get(id);
        if (set == null) {
            return null;
        } else {
            Counter counter = set.get(groupByValue);
            if (counter == null) {
                return null;
            }
            return counter.getValue();
        }
    }

    @Override
    public void setGroupByCounter(String id, String groupByValue, long value) {
        createOrGetGroupByDefaultCounter(id, groupByValue).set(value);
    }

    @Override
    public void incrementGroupByAverageCounter(String id, String groupByValue, long sum, long count) {
        Map<String, AverageCounter> set = groupByAverageCounters.get(id);
        if (set == null) {
            Map<String, AverageCounter> value = new ConcurrentHashMap();
            AverageCounter aLong = new AverageCounter(sum, count);
            value.put(groupByValue, aLong);
            groupByAverageCounters.put(id, value);
        } else {
            AverageCounter strings = set.get(groupByValue);
            if (strings == null) {
                AverageCounter objects = new AverageCounter(sum, count);
                set.put(groupByValue, objects);
            } else {
                strings.add(sum, count);
            }
        }
    }

    @Override
    public void incrementCounter(String id, long incrementBy) {
        getInternalCounter(id).addAndGet(incrementBy);
    }

    @Override
    public Long getCounter(String id) {
        AtomicLong atomicLong = counters.get(id);
        if (atomicLong == null) {
            return null;
        } else {
            return atomicLong.get();
        }
    }

    @Override
    public void setCounter(String id, long target) {
        getInternalCounter(id).set(target);
    }

    @Override
    public void addSet(String id, String item) {
        getInternalSet(id).add(item);
    }

    @Override
    public void incrementAverageCounter(String id, long sum, int count) {
        getInternalCounter(id + ":sum").addAndGet(sum);
        getInternalCounter(id + ":count").addAndGet(count);
    }

    @Override
    public AverageCounter getAverageCounter(String id) {
        return new AverageCounter(getInternalCounter(id + ":sum").get(), getInternalCounter(id + ":count").get());
    }

    public AtomicLong getInternalCounter(String id) {
        AtomicLong atomicLong = counters.get(id);
        if (atomicLong == null) {
            AtomicLong value = new AtomicLong();
            counters.put(id, value);
            return value;
        } else {
            return atomicLong;
        }
    }

    public Set<String> getInternalSet(String id) {
        Set<String> set = sets.get(id);
        if (set == null) {
            Set<String> value = new ConcurrentSkipListSet<>();
            sets.put(id, value);
            return value;
        } else {
            return set;
        }
    }

    public Set<String> getInternalGroupBySet(String id, String groupBy) {
        Map<String, Set<String>> set = groupBySets.get(id);
        if (set == null) {
            Map<String, Set<String>> value = new ConcurrentHashMap();
            HashSet<String> objects = new HashSet<>();
            value.put(groupBy, objects);
            groupBySets.put(id, value);
            return objects;
        } else {
            Set<String> strings = set.get(groupBy);
            if (strings == null) {
                HashSet<String> objects = new HashSet<>();
                set.put(groupBy, objects);
                return objects;
            }
            return strings;
        }
    }

    public SimpleCounter createOrGetGroupByDefaultCounter(String id, String groupBy) {
        Map<String, SimpleCounter> set = groupByDefaultCounters.get(id);
        if (set == null) {
            Map<String, SimpleCounter> value = new ConcurrentHashMap();
            SimpleCounter value1 = new SimpleCounter();
            value.put(groupBy, value1);
            groupByDefaultCounters.put(id, value);
            return value1;
        } else {
            SimpleCounter strings = set.get(groupBy);
            if (strings == null) {
                SimpleCounter objects = new SimpleCounter();
                set.put(groupBy, objects);
                return objects;
            }
            return strings;
        }
    }

    public AverageCounter createOrGetGroupByAverageCounter(String id, String groupBy) {
        Map<String, AverageCounter> set = groupByAverageCounters.get(id);
        if (set == null) {
            Map<String, AverageCounter> value = new ConcurrentHashMap<>();
            AverageCounter value1 = new AverageCounter();
            value.put(groupBy, value1);
            groupByAverageCounters.put(id, value);
            return value1;
        } else {
            AverageCounter strings = set.get(groupBy);
            if (strings == null) {
                AverageCounter objects = new AverageCounter();
                set.put(groupBy, objects);
                return objects;
            }
            return strings;
        }
    }

    @Override
    public Set<String> getSet(String id) {
        return sets.get(id);
    }

    public Map<String, Long> getGroupByCounters(String id) {
        Map<String, SimpleCounter> stringAtomicLongMap = groupByDefaultCounters.get(id);
        if (stringAtomicLongMap == null)
            return null;
        Map<String, Long> ret = new HashMap<>();
        for (String key : stringAtomicLongMap.keySet()) {
            ret.put(key, stringAtomicLongMap.get(key).getValue());
        }
        return ret;
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String id) {
        return groupBySets.get(id);
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String id) {
        Map<String, AverageCounter> stringAtomicLongMap = groupByAverageCounters.get(id);
        if (stringAtomicLongMap == null)
            return null;
        Map<String, AverageCounter> ret = new HashMap<>();
        for (String key : stringAtomicLongMap.keySet()) {
            ret.put(key, stringAtomicLongMap.get(key));
        }
        return ret;
    }

    public void flush() {
        sets.clear();
        counters.clear();
    }

}
