package org.rakam.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;
import org.rakam.util.SqlUtil;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

public abstract class MaterializedViewService {
    private final QueryMetadataStore database;
    private final QueryExecutor queryExecutor;
    private final char escapeIdentifier;

    public MaterializedViewService(QueryMetadataStore database, QueryExecutor queryExecutor, char escapeIdentifier) {
        this.database = database;
        this.queryExecutor = queryExecutor;
        this.escapeIdentifier = escapeIdentifier;
    }

    public abstract CompletableFuture<Void> create(RequestContext context, MaterializedView materializedView);

    public abstract CompletableFuture<QueryResult> delete(RequestContext context, String name);

    public Map<String, List<SchemaField>> getSchemas(RequestContext context, Optional<List<String>> names) {
        Map<String, CompletableFuture<List<SchemaField>>> futures = new HashMap<>();

        List<MaterializedView> materializedViews;
        if (names.isPresent()) {
            materializedViews = names.get().stream().map(name -> database.getMaterializedView(context.project, name)).collect(Collectors.toList());
        } else {
            materializedViews = database.getMaterializedViews(context.project);
        }

        CompletableFuture<List<SchemaField>>[] completableFutures = materializedViews.stream()
                .map(a -> {
                    CompletableFuture<List<SchemaField>> fut = new CompletableFuture<>();
                    metadata(context, a.query).whenComplete((schemaFields, throwable) -> {
                        if (throwable != null) {
                            schemaFields = ImmutableList.of();
                        }
                        fut.complete(schemaFields);
                    });
                    futures.put(a.tableName, fut);
                    return fut;
                })
                .toArray(CompletableFuture[]::new);
        CompletableFuture<Map<String, List<SchemaField>>> mapCompletableFuture = CompletableFuture.allOf(completableFutures).thenApply(result ->
                futures.entrySet().stream().collect(Collectors.toMap(key -> key.getKey(), key -> {
                    CompletableFuture<List<SchemaField>> value = key.getValue();
                    return value.join();
                })));
        return mapCompletableFuture.join();
    }

    public List<SchemaField> getSchema(RequestContext context, String tableName) {
        return metadata(context, database.getMaterializedView(context.project, tableName).query).join();
    }

    public void changeView(String project, String tableName, boolean realTime) {
        database.changeMaterializedView(project, tableName, realTime);
    }

    public abstract MaterializedViewExecution lockAndUpdateView(RequestContext context, MaterializedView materializedView);

    public List<MaterializedView> list(String project) {
        return database.getMaterializedViews(project);
    }

    public MaterializedView get(String project, String tableName) {
        return database.getMaterializedView(project, tableName);
    }

    public void replaceView(String project, MaterializedView view) {
        database.alter(project, view);
    }

    protected CompletableFuture<List<SchemaField>> metadata(RequestContext context, String query) {
        StringBuilder builder = new StringBuilder();
        Query queryStatement = (Query) SqlUtil.parseSql(query);
        CompletableFuture<List<SchemaField>> f = new CompletableFuture<>();

        try {
            new RakamSqlFormatter.Formatter(builder, qualifiedName -> queryExecutor.formatTableReference(context.project, qualifiedName, Optional.empty(), ImmutableMap.of()), escapeIdentifier)
                    .process(queryStatement, 1);
        } catch (Exception e) {
            f.completeExceptionally(e);
        }

        QueryExecution execution = queryExecutor.executeRawQuery(context, builder.toString() + " limit 0",
                ZoneOffset.UTC, ImmutableMap.of());
        execution.getResult().thenAccept(result -> {
            if (result.isFailed()) {
                f.completeExceptionally(new RakamException(result.getError().message, INTERNAL_SERVER_ERROR));
            } else {
                f.complete(result.getMetadata());
            }
        });
        return f;
    }

    public static class MaterializedViewExecution {
        public final Supplier<QueryExecution> materializedViewUpdateQuery;
        public final String computeQuery;
        public final boolean waitForUpdate;

        public MaterializedViewExecution(Supplier<QueryExecution> materializedViewUpdateQuery, String computeQuery, boolean waitForUpdate) {
            this.materializedViewUpdateQuery = materializedViewUpdateQuery;
            this.computeQuery = computeQuery;
            this.waitForUpdate = waitForUpdate;
        }
    }
}
