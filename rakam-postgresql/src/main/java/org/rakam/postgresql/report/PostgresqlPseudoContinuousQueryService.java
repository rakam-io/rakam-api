package org.rakam.postgresql.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class PostgresqlPseudoContinuousQueryService extends ContinuousQueryService {
    private final PostgresqlQueryExecutor executor;
    private final QueryExecutorService service;

    @Inject
    public PostgresqlPseudoContinuousQueryService(QueryMetadataStore database, QueryExecutorService service, PostgresqlQueryExecutor executor) {
        super(database);
        this.executor = executor;
        this.service = service;
    }

    @Override
    public CompletableFuture<QueryResult> create(ContinuousQuery report) {
        return executor.executeRawStatement(String.format("CREATE VIEW \"%s\".\"%s\" AS %s", report.project(), report.tableName, service.buildQuery(report.project(), report.query, null, new ArrayList<>())))
                .getResult().thenApply(result -> {
                    if (!result.isFailed()) {
                        database.createContinuousQuery(report);
                    } else {
                        throw new RakamException(result.getError().toString(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    }
                    return result;
                });
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String name) {
        return executor.executeRawStatement(String.format("DROP VIEW \"%s\".\"%s\"", project, name)).getResult().thenApply(result -> {
            if (!result.isFailed()) {
                database.deleteContinuousQuery(project, name);
            } else {
                throw new RakamException(result.getError().toString(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
            return true;
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        return database.getContinuousQueries(project).stream()
                .map(c -> new SimpleImmutableEntry<>(c, executor.executeRawQuery("select * from " + executor.formatTableReference(project, QualifiedName.of("continuous", c.tableName)) + " limit 0")))
                .collect(Collectors.toMap(entry -> entry.getKey().tableName, entry -> {
                    QueryResult join = entry.getValue().getResult().join();
                    if (join.isFailed()) {
                        return ImmutableList.of();
                    }
                    return join.getMetadata();
                }));
    }

    @Override
    public boolean test(String project, String query) {
        ContinuousQuery continuousQuery;
        try {
            continuousQuery = new ContinuousQuery(project, "test", "test",
                    query, ImmutableList.of(), ImmutableMap.of());
        } catch (ParsingException | IllegalArgumentException e) {
            throw new RakamException("Query is not valid: " + e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }

        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, qualifiedName ->
                executor.formatTableReference(project, qualifiedName))
                .process(continuousQuery.getQuery(), 1);

        QueryExecution execution = executor
                .executeRawQuery(builder.toString() + " limit 0");
        QueryResult result = execution.getResult().join();
        if (result.isFailed()) {
            throw new RakamException("Query error: " + result.getError().message, HttpResponseStatus.BAD_REQUEST);
        }
        return !result.isFailed();
    }
}
