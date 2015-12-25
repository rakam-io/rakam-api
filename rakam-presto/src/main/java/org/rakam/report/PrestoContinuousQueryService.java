package org.rakam.report;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.util.QueryFormatter;

import javax.inject.Inject;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PrestoContinuousQueryService extends ContinuousQueryService {

    public final static String PRESTO_STREAMING_CATALOG_NAME = "streaming";
    private final QueryMetadataStore database;
    private final PrestoQueryExecutor executor;

    @Inject
    public PrestoContinuousQueryService(QueryMetadataStore database, PrestoQueryExecutor executor, PrestoConfig config) {
        super(database);
        this.database = database;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        StringBuilder builder = new StringBuilder();

        new QueryFormatter(builder, name ->
                executor.formatTableReference(report.project, name)).process(report.getQuery(), 1);

        String prestoQuery = format("create view %s.\"%s\".\"%s\" as %s", PRESTO_STREAMING_CATALOG_NAME,
                report.project, report.tableName, builder.toString());

        ImmutableMap<String, String> sessionParameter = ImmutableMap.of(PRESTO_STREAMING_CATALOG_NAME + ".partition_keys",
                Joiner.on(",").join(report.partitionKeys));

        return executor.executeRawQuery(prestoQuery, sessionParameter)
                .getResult().thenApply(result -> {
            if (result.getError() == null) {
                database.createContinuousQuery(report);
                return QueryResult.empty();
            }
            return QueryResult.empty();
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String name) {
        ContinuousQuery continuousQuery = database.getContinuousQuery(project, name);

        String prestoQuery = format("drop view %s.\"%s\".\"%s\"", PRESTO_STREAMING_CATALOG_NAME,
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
        List<SimpleImmutableEntry<String, CompletableFuture<QueryResult>>> collect = database.getContinuousQueries(project).stream()
                .map(query -> {
                    PrestoQueryExecution prestoQueryExecution = executor.executeRawQuery(format("select * from %s.\"%s\".\"%s\" limit 0",
                            PRESTO_STREAMING_CATALOG_NAME, project, query.tableName));
                    return new SimpleImmutableEntry<>(query.tableName, prestoQueryExecution
                            .getResult());
                }).collect(Collectors.toList());

        CompletableFuture.allOf(collect.stream().map(c -> c.getValue()).toArray(CompletableFuture[]::new)).join();

        ImmutableMap.Builder<String, List<SchemaField>> builder = ImmutableMap.builder();
        for (SimpleImmutableEntry<String, CompletableFuture<QueryResult>> entry : collect) {
            QueryResult join = entry.getValue().join();
            if(join.isFailed()) {
                continue;
            }
            builder.put(entry.getKey(), join.getMetadata());
        }
        return builder.build();
    }

    @Override
    public boolean test(String project, String query) {
        return true;
    }

}
