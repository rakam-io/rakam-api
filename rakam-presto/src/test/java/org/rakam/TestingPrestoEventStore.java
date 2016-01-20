package org.rakam;

import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.report.PrestoConfig;
import org.rakam.report.PrestoQueryExecutor;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.rakam.report.PrestoQueryExecution.PRESTO_TIMESTAMP_FORMAT;

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
                event.collection(), buildValues(event.properties())));
    }

    @Override
    public void storeBatch(List<Event> events) {
        for (Map.Entry<String, List<Event>> collection : events.stream().collect(Collectors.groupingBy(e -> e.collection())).entrySet()) {
            queryExecutor.executeRawStatement(String.format("INSERT INTO %s.%s.%s VALUES %s",
                    config.getColdStorageConnector(), events.get(0).project(),
                    collection.getKey(),
                    collection.getValue().stream().map(e -> buildValues(e.properties())).collect(Collectors.joining(", "))))
                    .getResult().join();
        }
    }

    private String buildValues(GenericRecord properties) {
        StringBuilder builder = new StringBuilder("(");
        int size = properties.getSchema().getFields().size();

        if(size > 0) {
            appendValue(builder, properties.get(0));
            for (int i = 1; i < size; i++) {
                builder.append(", ");
                appendValue(builder, properties.get(i));
            }
        }

        return builder.append(")").toString();
    }

    private void appendValue(StringBuilder builder, Object value) {
        if(value instanceof String) {
            builder.append("'").append(value).append("'");
        } else
        if(value instanceof Number) {
            builder.append(String.format("%.2f", ((Number) value).doubleValue()));
        } else
        if(value instanceof Boolean) {
            builder.append(value);
        } else
        if(value instanceof Map) {
            builder.append("MAP(");
            appendValue(builder, ((Map) value).keySet());
            builder.append(", ");
            appendValue(builder, ((Map) value).values());
            builder.append(")");
        } else
        if(value instanceof Collection) {
            builder.append("ARRAY [");
            for (Object item : ((Collection) value)) {
                appendValue(builder, item);
            }
            builder.append("]");
        } else
        if(value instanceof Instant) {
            builder.append("timestamp '").append(PRESTO_TIMESTAMP_FORMAT.format(((Instant) value).atZone(ZoneId.systemDefault()))).append('\'');
        } else
        if(value instanceof LocalDate) {
            builder.append("date '").append(value).append('\'');
        }
    }
}
