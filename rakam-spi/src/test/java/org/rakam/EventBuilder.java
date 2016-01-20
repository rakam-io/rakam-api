package org.rakam;

import com.google.common.collect.ImmutableSet;
import org.apache.avro.generic.GenericData;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.util.AvroUtil;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class EventBuilder {
    private final Metastore metastore;
    private final String project;
    private final Map<String, Set<SchemaField>> fieldCache = new ConcurrentHashMap<>();

    public EventBuilder(String project, Metastore metastore) {
        this.project = project;
        this.metastore = metastore;
    }

    public Event createEvent(String collection, Map<String, Object> properties) {
        List<SchemaField> fields = generateSchema(properties);
        Set<SchemaField> cache = fieldCache.get(collection);
        if (cache == null || !fields.stream().allMatch(f -> cache.contains(f))) {
            ImmutableSet<SchemaField> fieldsSet = ImmutableSet.copyOf(fields);
            fields = metastore.getOrCreateCollectionFieldList(project, collection, fieldsSet);
            fieldCache.put(collection, fieldsSet);
        }

        GenericData.Record record = new GenericData.Record(AvroUtil.convertAvroSchema(fields));
        properties.forEach((key, value) -> record.put(key, value));

        return new Event(project, collection, new Event.EventContext(null, null, null, null), record);
    }

    private List<SchemaField> generateSchema(Map<String, Object> properties) {
        return properties.entrySet().stream()
                .map(entry -> new SchemaField(entry.getKey(), getType(entry.getValue())))
                .collect(Collectors.toList());
    }

    public static FieldType getType(Object value) {
        if(value instanceof String) {
            return FieldType.STRING;
        }
        if(value instanceof Number) {
            return FieldType.DOUBLE;
        }
        if(value instanceof Boolean) {
            return FieldType.BOOLEAN;
        }
        if(value instanceof Map) {
            Iterator<Map.Entry> iterator = ((Map) value).entrySet().iterator();
            if(!iterator.hasNext()) {
                throw new UnsupportedOperationException("empty map");
            }
            Map.Entry next = iterator.next();
            if(!(next.getKey() instanceof String)) {
                throw new UnsupportedOperationException("the key of map value must be string");
            }
            return getType(next.getValue()).convertToMapValueType();
        }

        if(value instanceof List) {
            Iterator iterator = ((List) value).iterator();
            if(!iterator.hasNext()) {
                throw new UnsupportedOperationException("empty map");
            }
            return getType(iterator.next()).convertToArrayType();
        }

        if(value instanceof Instant) {
            return FieldType.TIMESTAMP;
        }

        if(value instanceof LocalDate) {
            return FieldType.DATE;
        }

        throw new IllegalArgumentException();
    }
}
