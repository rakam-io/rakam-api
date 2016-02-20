package org.rakam.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
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

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;


public abstract class MaterializedViewService {
    private final SqlParser parser = new SqlParser();

    protected final QueryMetadataStore database;
    protected final QueryExecutor queryExecutor;
    private final Clock clock;

    public MaterializedViewService(QueryExecutor queryExecutor, QueryMetadataStore database, Clock clock) {
        this.database = database;
        this.queryExecutor = queryExecutor;
        this.clock = clock;
    }

    public CompletableFuture<Void> create(MaterializedView materializedView) {
        return metadata(materializedView.project, materializedView.query)
                .thenAccept(metadata -> database.createMaterializedView(materializedView));
    }

    private QueryExecution update(MaterializedView materializedView) {
        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (parser) {
            statement = (Query) parser.createStatement(materializedView.query);
        }

        new QueryFormatter(builder, a -> queryExecutor.formatTableReference(materializedView.project, a)).process(statement, 1);

        String query;

        if(materializedView.incrementalField == null || materializedView.lastUpdate == null) {
            query = format("CREATE TABLE %s AS (%s)",
                    queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName)),
                    builder.toString());
        } else {
            query = format("INSERT INTO %s SELECT * FROM (%s) WHERE %s > from_unixtime(%d)",
                    queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName)),
                    builder.toString(),
                    materializedView.incrementalField,
                    materializedView.lastUpdate.getEpochSecond());
        }


        return queryExecutor.executeRawStatement(query);
    }

    public CompletableFuture<QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        String reference = queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName));
        return queryExecutor.executeRawQuery(format("DELETE TABLE %s",
                reference)).getResult();
    }

    public List<MaterializedView> list(String project) {
        return database.getMaterializedViews(project);
    }

    public MaterializedView get(String project, String tableName) {
        return database.getMaterializedView(project, tableName);
    }

    private CompletableFuture<List<SchemaField>> metadata(String project, String query) {
        StringBuilder builder = new StringBuilder();
        Query queryStatement = (Query) parser.createStatement(checkNotNull(query, "query is required"));

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

    public QueryExecution lockAndUpdateView(MaterializedView materializedView) {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        boolean availableForUpdating = database.updateMaterializedView(materializedView, f);
        if(availableForUpdating) {
            String reference = queryExecutor.formatTableReference(materializedView.project, QualifiedName.of("materialized", materializedView.tableName));

            if (materializedView.lastUpdate != null) {
                QueryExecution execution;
                if(materializedView.incrementalField == null) {
                    execution = queryExecutor.executeRawStatement(format("DROP TABLE %s", reference));
                } else {
                    execution = queryExecutor.executeRawStatement(String.format("DELETE FROM %s WHERE %s > from_unixtime(%d)",
                            reference, materializedView.incrementalField, materializedView.lastUpdate.getEpochSecond()));
                }

                QueryResult result = execution.getResult().join();
                if (result.isFailed()) {
                    return execution;
                }
            }

            QueryExecution queryExecution = update(materializedView);

            queryExecution.getResult().thenAccept(result -> f.complete(!result.isFailed()));

            return queryExecution;
        }

        return null;
    }

    public boolean needsUpdate(MaterializedView m) {
        return m.lastUpdate == null || m.lastUpdate.until(clock.instant(), ChronoUnit.MILLIS) > m.updateInterval.toMillis();
    }
}
