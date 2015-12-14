package org.rakam.report.postgresql;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.postgresql.PostgresqlMetastore;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class PostgresqlPseudoContinuousQueryService extends ContinuousQueryService {
    private final PostgresqlQueryExecutor executor;
    private final PostgresqlMetastore metastore;

    @Inject
    public PostgresqlPseudoContinuousQueryService(QueryMetadataStore database, PostgresqlQueryExecutor executor, PostgresqlMetastore metastore) {
        super(database);
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
        return executor.executeRawQuery(String.format("DROP VIEW \"%s\".\"%s\"", project, name)).getResult().thenApply(result -> !result.isFailed());
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        return database.getContinuousQueries(project).stream()
                .map(c -> new SimpleImmutableEntry<>(c, executor.executeRawQuery("select * from "+executor.formatTableReference(project, QualifiedName.of("continuous", c.tableName)) + " limit 0")))
                .collect(Collectors.toMap(entry -> entry.getKey().tableName, entry -> {
                    QueryResult join = entry.getValue().getResult().join();
                    if(join.isFailed()) {
                        return ImmutableList.of();
                    }
                    return join.getMetadata();
                }));
    }

    @Override
    public List<SchemaField> test(String project, String query) {
        ContinuousQuery continuousQuery;
        try {
            continuousQuery = new ContinuousQuery(project, "test", "test",
                    query, ImmutableList.of(), ImmutableMap.of());
        } catch (ParsingException|IllegalArgumentException e) {
            throw new RakamException("Query is not valid: "+e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }

        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, qualifiedName ->
                executor.formatTableReference(project, qualifiedName))
                .process(continuousQuery.getQuery(), 1);

        QueryExecution execution = executor
                .executeRawQuery(builder.toString() + " limit 0");
        QueryResult result = execution.getResult().join();
        if(result.isFailed()) {
            throw new RakamException("Query error: "+result.getError().message, HttpResponseStatus.BAD_REQUEST);
        }
        return result.getMetadata();
    }
}
