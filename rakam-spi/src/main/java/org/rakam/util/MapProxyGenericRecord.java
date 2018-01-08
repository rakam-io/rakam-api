package org.rakam.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class MapProxyGenericRecord
        implements GenericRecord {

    private final ObjectNode properties;

    public MapProxyGenericRecord(ObjectNode properties) {
        this.properties = properties;
    }

    @Override
    public Schema getSchema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(int i, Object v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(String key, Object v) {
        if (v instanceof String) {
            properties.put(key, (String) v);
        } else if (v instanceof Integer) {
            properties.put(key, (Integer) v);
        } else if (v instanceof Float) {
            properties.put(key, (Float) v);
        } else if (v instanceof Double) {
            properties.put(key, (Double) v);
        } else if (v instanceof Long) {
            properties.put(key, (Long) v);
        } else if (v instanceof byte[]) {
            properties.put(key, (byte[]) v);
        } else if (v instanceof Boolean) {
            properties.put(key, (Boolean) v);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Object get(String key) {
        return properties.get(key);
    }
}
