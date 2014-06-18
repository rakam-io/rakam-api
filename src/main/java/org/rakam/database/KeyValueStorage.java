package org.rakam.database;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by buremba on 27/05/14.
 */
public interface KeyValueStorage {
    public abstract void incrementCounter(String key, long increment);
    public abstract void setCounter(String s, long target);
    public abstract void addSet(String setName, String item);
    public abstract void removeSet(String setName);
    public abstract void removeCounter(String setName);
    public abstract Long getCounter(String key);
    public abstract int getSetCount(String key);
    public abstract Iterator<String> getSetIterator(String key);
    public abstract Set<String> getSet(String key);
    public abstract void incrementCounter(String key);
    public abstract void addSet(String setName, Collection<String> items);
    public abstract void addGroupByItem(String aggregation, String groupBy, String item);
    public abstract void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy);

    public abstract void flush();
}
