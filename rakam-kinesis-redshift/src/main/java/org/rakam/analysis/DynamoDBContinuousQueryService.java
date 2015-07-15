package org.rakam.analysis;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.Page;
import com.google.inject.Inject;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import org.apache.avro.generic.GenericRecord;
import org.rakam.analysis.stream.ContinuousQueryExecutor;
import org.rakam.analysis.stream.QueryAnalyzer;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:25.
 */
public class DynamoDBContinuousQueryService extends ContinuousQueryService {
    private final QueryAnalyzer queryAnalyzer;
    private final IMap<String, Operator> queryMap;
    private final Metastore metastore;

    @Inject
    public DynamoDBContinuousQueryService(QueryMetadataStore database, Metastore metastore) {
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

    public void updateReport(ContinuousQuery report, List<GenericRecord> records) {
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

        ContinuousQueryExecutor plan = queryAnalyzer.planPartial(report.query, records, commonColumns, new Consumer<Page>() {
            @Override
            public void accept(Page page) {
                queryMap.executeOnKey(report.name, new StringOperatorEntryProcessor(report, commonColumns, page));
            }
        });
        plan.execute();
    }

    public QueryResult executeQuery(String tableName, String sql) {
        return (QueryResult) queryMap.executeOnKey(tableName, new StreamQueryProcessor(sql));
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        database.createContinuousQuery(report);
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        return null;
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String s) {
        return null;
    }

}
