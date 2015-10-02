package org.rakam.collection;

import java.util.List;


public interface EventProperty {
    public Object get(String key);
    public List<SchemaField> getSchema();
}
