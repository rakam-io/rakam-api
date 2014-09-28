package org.rakam.cache;

import org.rakam.analysis.AverageCounter;
import org.rakam.cache.hazelcast.models.Counter;
import org.rakam.cache.hazelcast.models.SimpleCounter;
import org.rakam.database.KeyValueStorage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by buremba on 21/12/13.
 */
public interface CacheAdapter extends KeyValueStorage {
    void incrementGroupBySimpleCounter(String aggregation, String groupBy, long incrementBy);
    void addGroupByString(String id, String groupByValue, String s);
    void addGroupByString(String id, String groupByValue, Collection<String> s);
    void removeGroupByCounters(String key);
    Map<String, Long> getGroupByCounters(String key);
    Map<String, Set<String>> getGroupByStrings(String key);
    Map<String, Set<String>> getGroupByStrings(String key, int limit);

    Map<String, Long> getGroupByCounters(String key, int limit);
    void incrementGroupByAverageCounter(String id, String key, long sum, long counter);
    void incrementAverageCounter(String id, long sum, long counter);
    AverageCounter getAverageCounter(String id);
    void removeGroupByStrings(String key);
    Map<String, Long> getGroupByStringsCounts(String key, Integer items);

    Map<String, AverageCounter> getGroupByAverageCounters(String rule_id);

    Map<String, AverageCounter> getGroupByAverageCounters(String key, int limit);

    default void addAll(Map<String, Counter> counters) {
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            Counter counter = entry.getValue();
            if(counter instanceof SimpleCounter) {
                incrementCounter(entry.getKey(), counter.getValue());
            }else
            if(counter instanceof AverageCounter) {
                AverageCounter c = (AverageCounter) counter;
                incrementAverageCounter(entry.getKey(), c.getSum(), c.getCount());
            }else {
                throw new IllegalStateException("counter type is not found");
            }
        }
    }
}
