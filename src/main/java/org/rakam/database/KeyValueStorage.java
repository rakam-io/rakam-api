package org.rakam.database;

import java.util.Collection;
import java.util.Set;

/**
 * Created by buremba on 27/05/14.
 */
public interface KeyValueStorage {
    public void incrementCounter(String key, long increment);
    public void setCounter(String s, long target);
    public void addSet(String setName, String item);
    public void removeSet(String setName);
    public void removeCounter(String setName);
    public Long getCounter(String key);
    public int getSetCount(String key);
    public Set<String> getSet(String key);
    public void incrementCounter(String key);
    public void addSet(String setName, Collection<String> items);
    public void flush();
}
