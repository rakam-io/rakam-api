package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class QueryExecutorService {
    private final SqlParser parser = new SqlParser();

    private final QueryExecutor executor;
    private final QueryMetadataStore queryMetadataStore;
    private final MaterializedViewService materializedViewService;
    private final Metastore metastore;
    private volatile Set<String> projectCache;

    @Inject
    public QueryExecutorService(QueryExecutor executor, QueryMetadataStore queryMetadataStore, Metastore metastore, MaterializedViewService materializedViewService) {
        this.executor = executor;
        this.queryMetadataStore = queryMetadataStore;
        this.materializedViewService = materializedViewService;
        this.metastore = metastore;
    }

    public QueryExecution executeQuery(String project, String sqlQuery, int limit) {
        if (!projectExists(project)) {
            throw new IllegalArgumentException("Project is not valid");
        }
        List<MaterializedView> materializedViews = new ArrayList<>();
        String query;

        try {
            query = buildQuery(project, sqlQuery, limit, materializedViews);
        } catch (ParsingException e) {
            return QueryExecution.completedQueryExecution(sqlQuery, QueryResult.errorResult(new QueryError(e.getMessage(), null, null, e.getLineNumber(), e.getColumnNumber())));
        }

        List<Map.Entry<MaterializedView, QueryExecution>> queryExecutions = materializedViews.stream()
                .filter(m -> materializedViewService.needsUpdate(m))
                .map(m -> new AbstractMap.SimpleImmutableEntry<>(m, materializedViewService.lockAndUpdateView(m)))
                // there may be processes updating this materialized views
                .filter(m -> m.getValue() != null)
                .collect(Collectors.toList());

        if(queryExecutions.isEmpty()) {
            QueryExecution execution = executor.executeRawQuery(query);
            if(materializedViews.isEmpty()) {
                return execution;
            } else {
                Map<String, Long> collect = materializedViews.stream().collect(Collectors.toMap(v -> v.name, v -> v.lastUpdate != null ? v.lastUpdate.toEpochMilli() : -1));
                return new DelegateQueryExecution(execution, result -> {
                    result.setProperty("materializedViews", collect);
                    return result;
                });
            }
        } else {
            CompletableFuture<QueryExecution> mergedQueries = CompletableFuture.allOf(queryExecutions.stream()
                    .filter(e -> e.getValue() != null)
                    .map(e -> e.getValue().getResult())
                    .toArray(CompletableFuture[]::new)).thenApply((r) -> {

                for (Map.Entry<MaterializedView, QueryExecution> queryExecution : queryExecutions) {
                    QueryResult result = queryExecution.getValue().getResult().join();
                    if (result.isFailed()) {
                        return new DelegateQueryExecution(queryExecution.getValue(),
                                materializedQueryUpdateResult -> {
                                    QueryError error = materializedQueryUpdateResult.getError();
                                    String message = String.format("Error while updating materialized table '%s': %s", queryExecution.getKey().tableName, error.message);
                                    return QueryResult.errorResult(new QueryError(message, error.sqlState, error.errorCode, error.errorLine, error.charPositionInLine));
                                });
                    }
                }

                return executor.executeRawQuery(query);
            });

            return new QueryExecution() {
                @Override
                public QueryStats currentStats() {
                    QueryStats currentStats = null;
                    for (Map.Entry<MaterializedView, QueryExecution> queryExecution : queryExecutions) {
                        QueryStats queryStats = queryExecution.getValue().currentStats();
                        if(currentStats == null) {
                            currentStats = queryStats;
                        } else {
                            currentStats = merge(currentStats, queryStats);
                        }
                    }

                    if(mergedQueries.isDone()) {
                        currentStats = merge(currentStats, mergedQueries.join().currentStats());
                    }

                    return currentStats;
                }

                private QueryStats merge(QueryStats currentStats, QueryStats stats) {
                    return new QueryStats(currentStats.percentage+stats.percentage,
                            currentStats.state.equals(stats.state) ? currentStats.state : QueryStats.State.RUNNING,
                            Math.max(currentStats.node, stats.node),
                            stats.processedRows+currentStats.processedRows,
                            stats.processedBytes+currentStats.processedBytes,
                            stats.userTime+currentStats.userTime,
                            stats.cpuTime+currentStats.cpuTime,
                            stats.wallTime+currentStats.wallTime
                    );
                }

                @Override
                public boolean isFinished() {
                    if(mergedQueries.isDone()) {
                        QueryExecution join = mergedQueries.join();
                        return join == null || join.isFinished();
                    } else {
                        return false;
                    }
                }

                @Override
                public CompletableFuture<QueryResult> getResult() {
                    CompletableFuture<QueryResult> future = new CompletableFuture<>();
                    mergedQueries.thenAccept(r -> {
                        if (r == null) {
                            future.complete(null);
                        } else {
                            r.getResult().thenAccept(result -> {
                                if (!result.isFailed()) {
                                    Map<String, Long> collect = materializedViews.stream().collect(Collectors.toMap(v -> v.name, v -> v.lastUpdate.toEpochMilli()));
                                    result.setProperty("materializedViews", collect);
                                }

                                future.complete(result);
                            });
                        }
                    });
                    return future;
                }

                @Override
                public String getQuery() {
                    return query;
                }

                @Override
                public void kill() {
                    for (Map.Entry<MaterializedView, QueryExecution> queryExecution : queryExecutions) {
                        queryExecution.getValue().kill();
                    }
                    mergedQueries.thenAccept(q -> q.kill());
                }
            };
        }
    }

    public QueryExecution executeQuery(String project, String sqlQuery) {
        if (!projectExists(project)) {
            throw new IllegalArgumentException("Project is not valid");
        }

        List<MaterializedView> materializedViews = new ArrayList<>();
        String query = buildQuery(project, sqlQuery, null, materializedViews);
        return executor.executeRawQuery(query);
    }

    public QueryExecution executeStatement(String project, String sqlQuery) {
        List<MaterializedView> views = new ArrayList<>();
        String query = buildStatement(project, sqlQuery, views);

        return executor.executeRawStatement(query);
    }

    private synchronized void updateProjectCache() {
        projectCache = metastore.getProjects();
    }

    private boolean projectExists(String project) {
        if (projectCache == null) {
            updateProjectCache();
        }

        if (!projectCache.contains(project)) {
            updateProjectCache();
            if (!projectCache.contains(project)) {
                return false;
            }
        }

        return true;
    }

    public String buildQuery(String project, String query, Integer maxLimit, List<MaterializedView> materializedViews) {
        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (parser) {
            statement = (Query) parser.createStatement(query);
        }

        new QueryFormatter(builder, tableNameMapper(project, materializedViews)).process(statement, 1);

        if (maxLimit != null) {
            Integer limit = null;
            if(statement.getLimit().isPresent()) {
                limit = Integer.parseInt(statement.getLimit().get());
            }
            if(statement.getQueryBody() instanceof QuerySpecification && ((QuerySpecification) statement.getQueryBody()).getLimit().isPresent()) {
                limit = Integer.parseInt(((QuerySpecification) statement.getQueryBody()).getLimit().get());
            }
            if(limit != null) {
                if(limit > maxLimit) {
                    throw new IllegalArgumentException(format("The maximum value of LIMIT statement is %s", statement.getLimit().get()));
                }
            } else {
                builder.append(" LIMIT ").append(maxLimit);
            }
        }

        return builder.toString();
    }

    private Function<QualifiedName, String> tableNameMapper(String project, List<MaterializedView> materializedViews) {
        return node -> {
            if (node.getPrefix().isPresent() && node.getPrefix().get().toString().equals("materialized")) {
                MaterializedView materializedView = queryMetadataStore.getMaterializedView(project, node.getSuffix());
                materializedViews.add(materializedView);
            }
            return executor.formatTableReference(project, node);
        };
    }

    private String buildStatement(String project, String query, List<MaterializedView> views) {
        StringBuilder builder = new StringBuilder();
        com.facebook.presto.sql.tree.Statement statement;
        synchronized (parser) {
            statement = parser.createStatement(query);
        }

        new QueryFormatter(builder, tableNameMapper(project, views)).process(statement, 1);

        return builder.toString();
    }

    public CompletableFuture<List<SchemaField>> metadata(String project, String query) {
        StringBuilder builder = new StringBuilder();
        Query queryStatement = (Query) parser.createStatement(checkNotNull(query, "query is required"));

        new QueryFormatter(builder, qualifiedName -> executor.formatTableReference(project, qualifiedName))
                .process(queryStatement, 1);

        QueryExecution execution = executor
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
