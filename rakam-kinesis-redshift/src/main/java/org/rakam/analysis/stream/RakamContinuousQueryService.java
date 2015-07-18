package org.rakam.analysis.stream;

import com.facebook.presto.spi.Page;
import com.google.inject.Inject;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import org.rakam.analysis.stream.processor.Processor;
import org.rakam.collection.Event;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryResult;
import org.rakam.util.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:25.
 */
public class RakamContinuousQueryService extends ContinuousQueryService {
    private final QueryAnalyzer queryAnalyzer;
    private final IMap<Tuple<String, String>, Processor> queryMap;
    private final Metastore metastore;

    @Inject
    public RakamContinuousQueryService(QueryMetadataStore database, Metastore metastore) {
        super(database);
        this.queryAnalyzer = new QueryAnalyzer();
        this.metastore = metastore;
        Config config = new Config();
        config.getMapConfig( "default" )
                .setInMemoryFormat(InMemoryFormat.OBJECT);

        Collection<SerializerConfig> serializerConfigs = config.getSerializationConfig().getSerializerConfigs();
        serializerConfigs.add(new SerializerConfig()
                        .setTypeClass(Page.class)
                        .setImplementation(new PageSerializer(queryAnalyzer.getMetadata())));
        serializerConfigs.add(new SerializerConfig()
                        .setTypeClass(QueryResult.class)
                        .setImplementation(new QueryResultSerializer()));

        queryMap = Hazelcast.newHazelcastInstance(config).getMap("continuousQueries");
    }

    public void updateReport(ContinuousQuery report, Iterable<Event> records) {
        Stream<SchemaField> fields;
        if(report.collections.size() == 1) {
            fields = metastore.getCollection(report.project, report.collections.get(0)).stream();
        }else {
            Stream<Map.Entry<String, List<SchemaField>>> collections = metastore.getCollections(report.project).entrySet().stream();
            if(report.collections.size() > 0) {
                collections = collections.filter(entry -> report.collections.contains(entry.getKey()));
            }
            fields = collections.flatMap(entry -> entry.getValue().stream());
        }

        List<SchemaField> commonColumns = fields.distinct().collect(Collectors.toList());

        ContinuousQueryExecutor plan = queryAnalyzer.executeIntermediate(report.query, records, commonColumns, page ->
                queryMap.executeOnKey(new Tuple<>(report.project, report.tableName), new AddInputEntryProcessor(report, commonColumns, page)));
        plan.execute();
    }

    public QueryResult executeQuery(String project, String tableName, String sql) {
        return (QueryResult) queryMap.executeOnKey(new Tuple(project, tableName), new StreamQueryExecutor(sql));
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        database.createContinuousQuery(report);
        return new QueryResult();
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        return null;
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        Set<Tuple<String, String>> collect = queryMap.keySet().stream()
                .filter(tuple -> tuple.v1().equals(project)).collect(Collectors.toSet());

        return queryMap.executeOnKeys(collect, new AbstractEntryProcessor<Tuple<String, String>, Processor>() {
            @Override
            public Object process(Map.Entry<Tuple<String, String>, Processor> entry) {
                return entry.getValue().getSchema();
            }
        }).entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().v2(),
                        entry -> (List<SchemaField>) entry.getValue()));
    }
}
