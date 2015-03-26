package org.rakam.collection.adapter.kafka;

import com.facebook.presto.jdbc.internal.guava.base.Joiner;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.inject.Inject;
import org.rakam.analysis.stream.EventStream;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.plugin.user.ConnectorFilterCriteria;
import org.rakam.plugin.user.FilterClause;
import org.rakam.plugin.user.FilterCriteria;
import org.rakam.report.PrestoConfig;
import org.rakam.report.PrestoQueryExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:13.
 */
public class KafkaStream implements EventStream {

    private final KafkaOffsetManager offsetManager;
    private final PrestoQueryExecutor prestoExecutor;
    private final PrestoConfig prestoConfig;
    private final EventSchemaMetastore metastore;

    @Inject
    public KafkaStream(KafkaOffsetManager offsetManager, EventSchemaMetastore metastore, PrestoQueryExecutor prestoExecutor, PrestoConfig prestoConfig) {
        this.offsetManager = offsetManager;
        this.prestoExecutor = prestoExecutor;
        this.prestoConfig = prestoConfig;
        this.metastore = metastore;
    }


    @Override
    public Supplier<CompletableFuture<Stream<Map<String, Object>>>> subscribe(String project, Map<String, FilterClause> collections, List<String> columns) {
        return new KafkaEventSupplier(project, collections, columns);
    }

    public class KafkaEventSupplier implements Supplier<CompletableFuture<Stream<Map<String, Object>>>> {
        private Map<String, Long> lastOffsets;
        private final Map<String, FilterClause> collections;
        private final String project;
        private final List<String> columns;

        private Map<String, List<SchemaField>> metadata;

        public KafkaEventSupplier(String project, Map<String, FilterClause> collections, List<String> columns) {
            this.lastOffsets = offsetManager.getOffset(project, collections.keySet());
            this.collections = collections;
            this.project = project;
            this.columns = columns;
        }

        private Map<String, List<SchemaField>> getMetadata() {
            if(metadata == null) {
                metadata = metastore.getSchemas(project).entrySet().stream()
                        .filter(entry -> collections.containsKey(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            return metadata;
        }

        private FieldType getColumnType(String collection, String column) {
            return getMetadata().get(collection).stream()
                    .filter(s -> s.getName().equals(column))
                    .findAny().get().getType();
        }

        @Override
        public CompletableFuture<Stream<Map<String, Object>>> get() {
            Map<String, Long> offsets = offsetManager.getOffset(project, collections.keySet());
            List<CompletableFuture> futures = new ArrayList<>(collections.size());
            AtomicReference<Stream<Map<String, Object>>> stream = new AtomicReference<>();

            if(columns == null) {
                collections.forEach((key, filter) -> {
                    List<String> schemaFields = getMetadata().get(key).stream()
                            .map(SchemaField::getName)
                            .collect(Collectors.toList());

                    CompletableFuture<Void> f = prestoExecutor.executeQuery(
                            format("select %s from %s where _offset >= %d and _offset < %d %s limit 1000",
                                    columns == null ? "*" : Joiner.on(", ").join(columns),
                                    prestoConfig.getHotStorageConnector() + "." + project + "." + key,
                                    this.lastOffsets.get(project + "_" + key.toLowerCase()),
                                    offsets.get(project + "_" + key.toLowerCase()),
                                    filter == null ? "" : buildPredicate(key, filter)))
                            .getResult()
                            .thenAccept((data) ->
                                    stream.getAndUpdate(mapStream -> {
                                        Stream<Map<String, Object>> s1 = data.getResult().stream()
                                                .map(row -> createMap(row, schemaFields));
                                        return mapStream == null ? s1 : Stream.concat(mapStream, s1);
                                    }));
                    futures.add(f);
                });
                return CompletableFuture.allOf(futures.stream().toArray(CompletableFuture[]::new))
                        .thenApply((v) -> {
                            lastOffsets = offsets;
                            return stream.get();
                        });
            } else {
                String query = collections.entrySet().stream().map(entry -> {
                    String collection = entry.getKey();
                    FilterClause filter = entry.getValue();
                    return format("select %s from %s where _offset >= %d and _offset < %d %s limit 1000",
                            columns == null ? "*" : Joiner.on(", ").join(columns),
                            prestoConfig.getHotStorageConnector() + "." + project + "." + collection,
                            this.lastOffsets.get(project + "_" + collection.toLowerCase()),
                            offsets.get(project + "_" + collection.toLowerCase()),
                            filter == null ? "" : buildPredicate(collection, filter));
                }).collect(Collectors.joining(" union "));

                return prestoExecutor.executeQuery(query).getResult()
                        .thenApply(r -> {
                            lastOffsets = offsets;
                            return r.getResult().stream().map(row -> createMap(row, columns));
                        });
            }
        }

        // TODO: find a way to return values without transforming the data.
        private Map createMap(List<Object> values, List<String> keys) {
            HashMap<String, Object> map = new HashMap();
            for (int i = 0; i < keys.size(); i++) {
                map.put(keys.get(i), values.get(i));
            }
            return map;
        }

        private String buildPredicate(String collection, FilterClause filter) {
            StringBuilder builder = new StringBuilder();
            build(builder, collection, filter);
            return builder.toString();
        }

        public void build(StringBuilder builder, String collection, FilterClause clause) {
            if(clause.isConnector()) {
                builder.append("(");
                for (FilterClause filterClause : ((ConnectorFilterCriteria) clause).getClauses()) {
                    build(builder, collection, filterClause);
                }
                builder.append(")");
            } else {
                FilterCriteria clause1 = (FilterCriteria) clause;
                builder.append(buildPredicate(clause1, getColumnType(collection, clause1.getAttribute())));
            }
        }

        private String buildPredicate(FilterCriteria filter, FieldType type) {
            Object o = convertPrestoValue(type, filter.getValue());

            switch (filter.getOperator()) {
                case $lte:
                    return format("%s <= %s", filter.getAttribute(), o);
                case $lt:
                    return format("%s <= %s", filter.getAttribute(), o);
                case $gt:
                    return format("%s > %s", filter.getAttribute(), o);
                case $gte:
                    return format("%s >= %s", filter.getAttribute(), o);
                case $contains:
                case $eq:
                    return format("%s = %s", filter.getAttribute(), o);
//            case $in:
                case $regex:
                    return format("%s ~ %s", filter.getAttribute(), o);
                case $starts_with:
                    return format("%s LIKE %s", filter.getAttribute(), o);
                case $is_null:
                    return format("%s is null", filter.getAttribute());
                case $is_set:
                    return format("%s is not null", filter.getAttribute());
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private Object convertPrestoValue(FieldType type, Object o) {
        switch (type) {
            case DATE:
                return format("date '%s'", o);
            case TIME:
                return format("time '%s'", o);
            case STRING:
                return format("'%s'", o);
            case LONG:
                return format("%d", o);
            case DOUBLE:
                return format("%f", o);
            case BOOLEAN:
                return format("%b", o);
            case ARRAY:
                return format("ARRAY [%s]", com.google.common.base.Joiner.on(", ").join(((ArrayNode) o)));
            default:
                throw new IllegalStateException();

        }
    }

}
