package org.rakam.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;


public abstract class MaterializedViewService {
    public final static SqlParser sqlParser = new SqlParser();
    private final QueryMetadataStore database;
    private final QueryExecutor queryExecutor;

    public MaterializedViewService(QueryMetadataStore database, QueryExecutor queryExecutor) {
        this.database = database;
        this.queryExecutor = queryExecutor;
    }

    public abstract CompletableFuture<Void> create(MaterializedView materializedView);

    public abstract CompletableFuture<QueryResult> delete(String project, String name);

    public CompletableFuture<Map<String, List<SchemaField>>> getSchemas(String project, Optional<List<String>> names) {
        Map<String, CompletableFuture<List<SchemaField>>> futures = new HashMap<>();

        List<MaterializedView> materializedViews;
        if(names.isPresent()) {
            materializedViews = names.get().stream().map(name -> database.getMaterializedView(project, name)).collect(Collectors.toList());
        } else {
            materializedViews = database.getMaterializedViews(project);
        }

        CompletableFuture<List<SchemaField>>[] completableFutures = materializedViews.stream()
                .map(a -> {
                    CompletableFuture<List<SchemaField>> metadata = metadata(project, a.query);
                    futures.put(a.tableName, metadata);
                    return metadata;
                })
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(completableFutures).thenApply(result ->
                futures.entrySet().stream().collect(Collectors.toMap(key -> key.getKey(), key -> {
                    CompletableFuture<List<SchemaField>> value = key.getValue();
                    return value.join();
                })));
    }

    public CompletableFuture<List<SchemaField>> getSchema(String project, String tableName) {
        return metadata(project, database.getMaterializedView(project, tableName).query);
    }

    public static class MaterializedViewExecution {
        public final QueryExecution queryExecution;
        public final String computeQuery;

        public MaterializedViewExecution(QueryExecution queryExecution, String computeQuery) {
            this.queryExecution = queryExecution;
            this.computeQuery = computeQuery;
        }
    }

    public abstract MaterializedViewExecution lockAndUpdateView(MaterializedView materializedView);

    public List<MaterializedView> list(String project) {
        return database.getMaterializedViews(project);
    }

    public MaterializedView get(String project, String tableName) {
        return database.getMaterializedView(project, tableName);
    }

    protected CompletableFuture<List<SchemaField>> metadata(String project, String query) {
        StringBuilder builder = new StringBuilder();
        Query queryStatement = (Query) sqlParser.createStatement(checkNotNull(query, "query is required"));

        new QueryFormatter(builder, qualifiedName -> queryExecutor.formatTableReference(project, qualifiedName))
                .process(queryStatement, 1);

        QueryExecution execution = queryExecutor
                .executeRawQuery(builder.toString() + " limit 0");
        CompletableFuture<List<SchemaField>> f = new CompletableFuture<>();
        execution.getResult().thenAccept(result -> {
            if (result.isFailed()) {
                f.completeExceptionally(new RakamException(result.getError().message, HttpResponseStatus.INTERNAL_SERVER_ERROR));
            } else {
                f.complete(result.getMetadata());
            }
        });
        return f;
    }
}
