package org.rakam;

import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.ValidationUtil;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.rakam.presto.analysis.PrestoQueryExecution.PRESTO_TIMESTAMP_FORMAT;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;

public class TestingPrestoEventStore implements SyncEventStore {
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
            QueryResult join = queryExecutor.executeRawStatement(String.format("INSERT INTO %s.%s.%s (_shard_time, %s) (%s)",
                    config.getColdStorageConnector(), checkProject(events.get(0).project(), '"'),
                    checkCollection(collection.getKey()),
                    collection.getValue().get(0).schema().stream().map(e -> ValidationUtil.checkCollection(e.getName()))
                            .collect(Collectors.joining(", ")),
                    collection.getValue().stream()
                            .map(e -> buildValues(e.properties(), e.schema()))
                            .collect(Collectors.joining(" union all "))))
                    .getResult().join();
            if (join.isFailed()) {
                try {
                    Thread.sleep(300000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                throw new IllegalStateException(join.getError().message);
            }

        }
        return EventStore.SUCCESSFUL_BATCH;
    }

    private String buildValues(GenericRecord properties, List<SchemaField> schema) {
        StringBuilder builder = new StringBuilder("select ");
        int size = properties.getSchema().getFields().size();

        appendValue(builder, Instant.now().toEpochMilli(), FieldType.TIMESTAMP);

        for (int i = 0; i < size; i++) {
            builder.append(", ");
            appendValue(builder, properties.get(i), schema.get(i).getType());
        }

        return builder.toString();
    }

    private void appendValue(StringBuilder builder, Object value, FieldType type) {
        switch (type) {
            case STRING:
                builder.append('\'').append(value).append('\'');
                break;
            case LONG:
                builder.append(((Number) value).longValue());
            case INTEGER:
                builder.append(((Number) value).intValue());
            case DECIMAL:
                builder.append(((Number) value).doubleValue());
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
                    builder.append("array [");
                    for (Object item : ((Collection) value)) {
                        appendValue(builder, item, type.getArrayElementType());
                    }
                    builder.append(']');
                } else if (type.isMap()) {
                    builder.append("map(");
                    appendValue(builder, ((Map) value).keySet(), FieldType.ARRAY_STRING);
                    builder.append(", ");
                    appendValue(builder, ((Map) value).values(), type.getMapValueType().convertToArrayType());
                    builder.append(')');
                }
        }
    }
}
