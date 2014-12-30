package org.rakam.stream.local;

import org.rakam.stream.AverageCounter;

import java.util.Map;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/10/14 14:40.
 */
public interface LocalCache {
    void incrementGroupByCounter(String id, String groupByValue, long incrementBy);

    void addGroupByString(String id, String groupByValue, String item);

    Long getGroupByCounter(String id, String groupByValue);

    void setGroupByCounter(String id, String groupByValue, long value);

    void incrementGroupByAverageCounter(String id, String groupByValue, long sum, long count);

    void incrementCounter(String id, long incrementBy);

    Long getCounter(String id);

    void setCounter(String id, long target);

    void addSet(String id, String item);

    void incrementAverageCounter(String id, long sum, int count);

    AverageCounter getAverageCounter(String id);

    Set<String> getSet(String id);

    Map<String, Long> getGroupByCounters(String id);

    Map<String, Set<String>> getGroupByStrings(String id);

    Map<String, AverageCounter> getGroupByAverageCounters(String s);

    void flush();
}
