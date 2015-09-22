package org.rakam.report;

import javax.inject.Inject;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.util.Tuple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 04:38.
 */
public class PrestoContinuousQueryService extends ContinuousQueryService {
    public final static String PRESTO_STREAMING_CATALOG_NAME = "streaming";
    private final QueryMetadataStore database;
    private final PrestoQueryExecutor executor;
    private final Metastore metastore;

    @Inject
    public PrestoContinuousQueryService(QueryMetadataStore database, PrestoQueryExecutor executor, Metastore metastore) {
        super(database);
        this.database = database;
        this.executor = executor;
        this.metastore = metastore;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        database.createContinuousQuery(report);
        return CompletableFuture.completedFuture(QueryResult.empty());
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String name) {
        ContinuousQuery continuousQuery = database.getContinuousQuery(project, name);

        String prestoQuery = format("drop view %s.%s.%s", PRESTO_STREAMING_CATALOG_NAME,
                continuousQuery.project, continuousQuery.tableName);
        return executor.executeRawQuery(prestoQuery).getResult().thenApply(result -> {
            if(result.getError() == null) {
                database.createContinuousQuery(continuousQuery);
                return true;
            }
            return false;
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        Function<List<Object>, SchemaField> listSchemaFieldFunction = row ->
                new SchemaField(
                        (String) row.get(0),
                        PrestoQueryExecution.fromPrestoType((String) row.get(1)),
                        (Boolean) row.get(2));

        return database.getContinuousQueries(project).parallelStream()
                .map(query -> new Tuple<String, QueryResult>(query.tableName, executor.executeRawQuery(
                        format("describe %s.%s.%s", PRESTO_STREAMING_CATALOG_NAME, project, query.tableName))
                        .getResult().join()))
                .collect(Collectors.toMap(item -> item.v1(), item -> item.v2().getResult().stream()
                        .map(listSchemaFieldFunction).collect(Collectors.toList())));

    }
}
