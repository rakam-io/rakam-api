package org.rakam.database;

import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/07/14 03:54.
 */
public interface OrderedSetStorage {
    public void addOrderedSet(String id, String key);
    public void removeOrderedSet(String id);
    public Set getOrderedSet(String id);
}
