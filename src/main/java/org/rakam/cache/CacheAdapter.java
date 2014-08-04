package org.rakam.cache;

import org.rakam.database.KeyValueStorage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by buremba on 21/12/13.
 */
public interface CacheAdapter extends KeyValueStorage {
    public boolean isOrdered();
    void addGroupByCounter(String aggregation, String groupBy);
    void addGroupByCounter(String aggregation, String groupBy, long incrementBy);
    void addGroupByString(String id, String groupByValue, String s);
    void addGroupByString(String id, String groupByValue, Collection<String> s);
    void removeGroupByCounters(String key);
    Map<String, Long> getGroupByCounters(String key);
    Map<String, Set<String>> getGroupByStrings(String key);

    Map<String, Set<String>> getGroupByStrings(String key, int limit);

    Map<String, Long> getGroupByCounters(String key, int limit);
    void incrementGroupByAverageCounter(String id, String key, long sum, long counter);
    void incrementAverageCounter(String id, long sum, long counter);
    org.rakam.cache.hazelcast.models.AverageCounter getAverageCounter(String id);
    void removeGroupByStrings(String key);

    Map<String, Long> getGroupByStringsCounts(String key, Integer items);
}
