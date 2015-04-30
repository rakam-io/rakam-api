package org.rakam.collection;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/04/15 00:33.
 */
public interface EventProperty {
    public Object get(String key);
    public List<SchemaField> getSchema();
}
