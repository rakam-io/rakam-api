package org.rakam;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.AvroUtil;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.collection.FieldType.*;

public class EventBuilder {
    private final Metastore metastore;
    private final String project;
    private final Map<String, List<SchemaField>> fieldCache = new ConcurrentHashMap<>();

    public EventBuilder(String project, Metastore metastore) {
        this.project = project;
        this.metastore = metastore;
    }

    public static FieldType getType(Object value) {
        if (value instanceof String) {
            return STRING;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Map) {
            Iterator<Map.Entry> iterator = ((Map) value).entrySet().iterator();
            if (!iterator.hasNext()) {
                throw new UnsupportedOperationException("empty map");
            }
            Map.Entry next = iterator.next();
            if (!(next.getKey() instanceof String)) {
                throw new UnsupportedOperationException("the key of map value must be string");
            }
            return getType(next.getValue()).convertToMapValueType();
        }

        if (value instanceof List) {
            Iterator iterator = ((List) value).iterator();
            if (!iterator.hasNext()) {
                throw new UnsupportedOperationException("empty map");
            }
            return getType(iterator.next()).convertToArrayType();
        }

        if (value instanceof Instant) {
            return TIMESTAMP;
        }

        if (value instanceof LocalDate) {
            return DATE;
        }

        throw new IllegalArgumentException(format("Undefined type: %s", value.getClass()));
    }

    public Event createEvent(String collection, Map<String, Object> properties) {
        List<SchemaField> cache = fieldCache.get(collection);

        List<SchemaField> fields;
        List<SchemaField> generatedSchema = generateSchema(properties);
        if (cache == null || !generatedSchema.stream().allMatch(f -> cache.contains(f))) {
            fields = metastore.getOrCreateCollectionFields(project, collection, ImmutableSet.copyOf(generatedSchema));
            fieldCache.put(collection, fields);
        } else {
            fields = cache;
        }

        try {
            GenericData.Record record = new GenericData.Record(AvroUtil.convertAvroSchema(fields));
            properties.forEach((key, value) -> record.put(key, cast(value,
                    record.getSchema().getField(key).schema().getTypes().get(1).getType())));

            return new Event(project, collection, Event.EventContext.empty(), fields, record);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void cleanCache() {
        fieldCache.clear();
    }

    private Object cast(Object value, Schema.Type type) {
        if (value instanceof Instant) {
            if (type != Schema.Type.LONG) {
                throw new IllegalStateException();
            }
            return ((Instant) value).toEpochMilli();
        }
        if (value instanceof LocalDate) {
            if (type != Schema.Type.INT) {
                throw new IllegalStateException();
            }
            return Ints.checkedCast(((LocalDate) value).toEpochDay());
        }

        if (type == Schema.Type.DOUBLE) {
            return ((Number) value).doubleValue();
        }
        if (type == Schema.Type.INT) {
            return ((Number) value).intValue();
        }
        if (type == Schema.Type.LONG) {
            return ((Number) value).longValue();
        }
        if (type == Schema.Type.STRING) {
            return value.toString();
        }
        return value;
    }

    private List<SchemaField> generateSchema(Map<String, Object> properties) {
        return properties.entrySet().stream()
                .map(entry -> new SchemaField(entry.getKey(), getType(entry.getValue())))
                .collect(Collectors.toList());
    }
}
