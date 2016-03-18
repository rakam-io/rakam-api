package org.rakam;

import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoQueryExecutor;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.rakam.presto.analysis.PrestoQueryExecution.PRESTO_TIMESTAMP_FORMAT;

public class TestingPrestoEventStore implements EventStore {
    private final PrestoQueryExecutor queryExecutor;
    private final PrestoConfig config;

    public TestingPrestoEventStore(PrestoQueryExecutor queryExecutor, PrestoConfig config) {
        this.queryExecutor = queryExecutor;
        this.config = config;
    }

    @Override
    public void store(Event event) {
        queryExecutor.executeRawStatement(String.format("INSERT INTO %s.%s.%s VALUES %s",
                config.getColdStorageConnector(), event.project(),
                event.collection(), buildValues(event.properties(), event.schema())));
    }

    @Override
    public int[] storeBatch(List<Event> events) {
        for (Map.Entry<String, List<Event>> collection : events.stream().collect(Collectors.groupingBy(e -> e.collection())).entrySet()) {
            queryExecutor.executeRawStatement(String.format("INSERT INTO %s.%s.%s VALUES %s",
                    config.getColdStorageConnector(), events.get(0).project(),
                    collection.getKey(),
                    collection.getValue().stream().map(e -> buildValues(e.properties(), e.schema())).collect(Collectors.joining(", "))))
                    .getResult().join();
        }
        return EventStore.SUCCESSFUL_BATCH;
    }

    private String buildValues(GenericRecord properties, List<SchemaField> schema) {
        StringBuilder builder = new StringBuilder("(");
        int size = properties.getSchema().getFields().size();

        if (size > 0) {
            appendValue(builder, properties.get(0), schema.get(0).getType());
            for (int i = 1; i < size; i++) {
                builder.append(", ");
                appendValue(builder, properties.get(i), schema.get(i).getType());
            }
        }

        return builder.append(")").toString();
    }

    private void appendValue(StringBuilder builder, Object value, FieldType type) {
        switch (type) {
            case STRING:
                builder.append('\'').append(value).append('\'');
                break;
            case LONG:
                builder.append(((Number) value).longValue());
                break;
            case DOUBLE:
                builder.append(String.format("%.2f", ((Number) value).doubleValue()));
                break;
            case BOOLEAN:
                builder.append(value);
                break;
            case TIMESTAMP:
                builder.append("timestamp '").append(PRESTO_TIMESTAMP_FORMAT.format(Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault()))).append('\'');
                break;
            case DATE:
                builder.append("date '").append(LocalDate.ofEpochDay((int) value)).append('\'');
                break;
            default:
                if (type.isArray()) {
                    builder.append("ARRAY [");
                    for (Object item : ((Collection) value)) {
                        appendValue(builder, item, type.getArrayElementType());
                    }
                    builder.append(']');
                } else if (type.isMap()) {
                    builder.append("MAP(");
                    appendValue(builder, ((Map) value).keySet(), FieldType.ARRAY_STRING);
                    builder.append(", ");
                    appendValue(builder, ((Map) value).values(), type.getMapValueType().convertToArrayType());
                    builder.append(')');
                }
        }
    }
}
