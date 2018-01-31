package org.rakam.kafka.collection;

import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.stream.CollectionStreamQuery;
import org.rakam.plugin.stream.EventStream;
import org.rakam.plugin.stream.StreamResponse;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoQueryExecutor;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class KafkaStream implements EventStream {

    private final KafkaOffsetManager offsetManager;
    private final PrestoQueryExecutor prestoExecutor;
    private final PrestoConfig prestoConfig;
    private final Metastore metastore;

    @Inject
    public KafkaStream(KafkaOffsetManager offsetManager, Metastore metastore, PrestoQueryExecutor prestoExecutor, PrestoConfig prestoConfig) {
        this.offsetManager = offsetManager;
        this.prestoExecutor = prestoExecutor;
        this.prestoConfig = prestoConfig;
        this.metastore = metastore;
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        return new KafkaEventSupplier(project, collections, columns, response);
    }

    public class KafkaEventSupplier implements EventStreamer {
        private final StreamResponse response;
        private final List<CollectionStreamQuery> collections;
        private final Set<String> collectionNames;
        private final String project;
        private final List<SchemaField> columns;
        private Map<String, Long> lastOffsets;
        private Map<String, List<SchemaField>> metadata;

        public KafkaEventSupplier(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
            this.collectionNames = collections.stream().map(c -> c.getCollection()).collect(Collectors.toSet());

            this.lastOffsets = offsetManager.getOffset(project, collectionNames);
            this.collections = collections;
            this.project = project;
            List<SchemaField> sample = getMetadata().get(collections.get(0));

            if (columns != null) {
                this.columns = columns.stream().map(colName -> sample.stream()
                        .filter(field -> field.getName().equals(colName)).findFirst().get())
                        .collect(Collectors.toList());
            } else {
                this.columns = null;
            }

            this.response = response;
        }

        private Map<String, List<SchemaField>> getMetadata() {
            if (metadata == null) {
                metadata = metastore.getCollections(project).entrySet().stream()
                        .filter(entry -> collectionNames.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            return metadata;
        }

        @Override
        public void sync() {
            Map<String, Long> offsets = offsetManager.getOffset(project, collectionNames);

            String query = collections.stream().map(e -> {
                String select;

                List<SchemaField> cols = columns != null ? columns : getMetadata().get(e.getCollection());
                select = String.format("'{\"_collection\": \"%s\",'||'", e.getCollection()) + cols.stream()
                        .map(field -> {
                            switch (field.getType()) {
                                case LONG:
                                case DOUBLE:
                                case INTEGER:
                                case DECIMAL:
                                case BOOLEAN:
                                    return format("\"%1$s\": '||COALESCE(cast(%1$s as varchar), 'null')||'", field.getName());
                                default:
                                    return format("\"%1$s\": \"'||COALESCE(replace(try_cast(%1$s as varchar), '\n', '\\n'), 'null')||'\"", field.getName());
                            }

                        })
                        .collect(Collectors.joining(", ")) + " }'";


                long offsetNow = offsets.get(project + "_" + e.getCollection().toLowerCase());
                long offsetBefore = this.lastOffsets.get(project + "_" + e.getCollection().toLowerCase());
                if (offsetBefore == offsetNow) {
                    return null;
                }
                return format("select %s from %s where _offset > %d and _offset <= %d %s",
                        select,
                        prestoConfig.getHotStorageConnector() + "." + project + "." + e.getCollection(),
                        offsetBefore,
                        offsetNow,
                        e.getFilter() == null ? "" : " AND " + e.getFilter().toString());

            }).filter(d -> d != null).collect(Collectors.joining(" union all "));

            if (query.isEmpty())
                return;

            prestoExecutor.executeRawQuery(new RequestContext(project, null), query + " limit 1000").getResult()
                    .thenAccept(r -> {
                        lastOffsets = offsets;
                        response.send("data", "[" + r.getResult().stream()
                                .map(s -> (String) s.get(0)).collect(Collectors.joining(",")) + "]");
                    });
        }

        @Override
        public void shutdown() {

        }

    }

//    private Object convertPrestoValue(FieldType type, Object o) {
//        switch (type) {
//            case DATE:
//                return format("date '%s'", o);
//            case TIME:
//                return format("time '%s'", o);
//            case TIMESTAMP:
//                return format("time '%s'", o);
//            case STRING:
//                return format("'%s'", o);
//            case LONG:
//                return format("%d", o);
//            case DOUBLE:
//                return format("%f", o);
//            case BOOLEAN:
//                return format("%b", o);
//            case ARRAY_STRING:
//                return format("ARRAY [%s]", com.google.common.base.Joiner.on(", ").join(((ArrayNode) o)));
//            default:
//                throw new IllegalStateException();
//
//        }
//    }

}
