package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.EscapeIdentifier;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.MaterializedViewService.MaterializedViewExecution;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecutor.Sample;
import org.rakam.util.NotExistsException;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;
import org.rakam.util.LogUtil;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.report.QueryResult.EXECUTION_TIME;

public class QueryExecutorService
{
    private final SqlParser parser = new SqlParser();
    public static final int MAX_QUERY_RESULT_LIMIT = 500000;

    private final QueryExecutor executor;
    private final MaterializedViewService materializedViewService;
    private final Metastore metastore;
    private final Clock clock;
    private final char escapeIdentifier;
    private volatile Set<String> projectCache;

    @Inject
    public QueryExecutorService(QueryExecutor executor, Metastore metastore, MaterializedViewService materializedViewService, Clock clock, @EscapeIdentifier char escapeIdentifier)
    {
        this.executor = executor;
        this.materializedViewService = materializedViewService;
        this.metastore = metastore;
        this.clock = clock;
        this.escapeIdentifier = escapeIdentifier;
    }

    public QueryExecution executeQuery(String project, String sqlQuery, Optional<Sample> sample, int limit)
    {
        if (!projectExists(project)) {
            throw new NotExistsException("Project");
        }
        HashMap<MaterializedView, MaterializedViewExecution> materializedViews = new HashMap<>();
        String query;

        try {
            query = buildQuery(project, sqlQuery, sample, limit, materializedViews);
        }
        catch (ParsingException e) {
            QueryError error = new QueryError(e.getMessage(), null, null, e.getLineNumber(), e.getColumnNumber());
            LogUtil.logQueryError(sqlQuery, error, executor.getClass());
            return QueryExecution.completedQueryExecution(sqlQuery, QueryResult.errorResult(error));
        }

        long startTime = System.currentTimeMillis();

        List<MaterializedViewExecution> queryExecutions = materializedViews.values().stream()
                .filter(m -> m.queryExecution != null)
                .collect(Collectors.toList());

        if (queryExecutions.isEmpty()) {
            QueryExecution execution = executor.executeRawQuery(query);
            if (materializedViews.isEmpty()) {
                return execution;
            }
            else {
                Map<String, Long> collect = materializedViews.entrySet().stream().collect(Collectors.toMap(v -> v.getKey().tableName, v -> v.getKey().lastUpdate != null ? v.getKey().lastUpdate.toEpochMilli() : -1));
                return new DelegateQueryExecution(execution, result -> {
                    result.setProperty("materializedViews", collect);
                    return result;
                });
            }
        }
        else {
            List<QueryExecution> executions = queryExecutions.stream()
                    .filter(e -> e.queryExecution != null)
                    .map(e -> e.queryExecution)
                    .collect(Collectors.toList());

            return new DelegateQueryExecution(new ChainQueryExecution(executions, query, (results) -> {
                for (MaterializedViewExecution queryExecution : queryExecutions) {
                    QueryResult result = queryExecution.queryExecution.getResult().join();
                    if (result.isFailed()) {
                        return new DelegateQueryExecution(queryExecution.queryExecution,
                                materializedQueryUpdateResult -> {
                                    QueryError error = materializedQueryUpdateResult.getError();
                                    String message = String.format("Error while updating materialized table '%s': %s", queryExecution.computeQuery, error.message);
                                    QueryError error1 = new QueryError(message, error.sqlState, error.errorCode, error.errorLine, error.charPositionInLine);
                                    LogUtil.logQueryError(query, error1, executor.getClass());
                                    return QueryResult.errorResult(error1);
                                });
                    }
                }

                return executor.executeRawQuery(query);
            }), result -> {
                if (!result.isFailed()) {
                    Map<String, Long> collect = materializedViews.entrySet().stream()
                            .collect(Collectors.toMap(
                                    v -> v.getKey().tableName,
                                    v -> Optional.ofNullable(v.getKey().lastUpdate).map(Instant::toEpochMilli).orElse(0L)));
                    result.setProperty("materializedViews", collect);
                    result.setProperty(EXECUTION_TIME, System.currentTimeMillis() - startTime);
                }

                return result;
            });
        }
    }

    public QueryExecution executeQuery(String project, String sqlQuery)
    {
        return executeQuery(project, sqlQuery, Optional.empty(), MAX_QUERY_RESULT_LIMIT);
    }

    public QueryExecution executeStatement(String project, String sqlQuery)
    {
        return executeQuery(project, sqlQuery);
    }

    private synchronized void updateProjectCache()
    {
        projectCache = metastore.getProjects();
    }

    private boolean projectExists(String project)
    {
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

    public String buildQuery(String project, String query, Optional<Sample> sample, Integer maxLimit, Map<MaterializedView, MaterializedViewExecution> materializedViews)
    {
        StringBuilder builder = new StringBuilder();
        Query statement;
        synchronized (parser) {
            statement = (Query) parser.createStatement(query);
        }

        // TODO: use fake StringBuilder for performance
        new QueryFormatter(new StringBuilder(), tableNameMapper(project, materializedViews, sample, true), escapeIdentifier).process(statement, 1);

        new QueryFormatter(builder, tableNameMapper(project, materializedViews, sample, false), escapeIdentifier).process(statement, 1);

        if (maxLimit != null) {
            Integer limit = null;
            if (statement.getLimit().isPresent()) {
                limit = Integer.parseInt(statement.getLimit().get());
            }
            if (statement.getQueryBody() instanceof QuerySpecification && ((QuerySpecification) statement.getQueryBody()).getLimit().isPresent()) {
                limit = Integer.parseInt(((QuerySpecification) statement.getQueryBody()).getLimit().get());
            }
            if (limit != null) {
                if (limit > maxLimit) {
                    throw new RakamException(format("The maximum value of LIMIT statement is %s", maxLimit), BAD_REQUEST);
                }
            }
            else {
                builder.append(" LIMIT ").append(maxLimit);
            }
        }

        return builder.toString();
    }

    private Function<QualifiedName, String> tableNameMapper(String project, Map<MaterializedView, MaterializedViewExecution> materializedViews, Optional<Sample> sample, boolean fetchReference)
    {
        return (node) -> {
            if (node.getPrefix().isPresent() && node.getPrefix().get().toString().equals("materialized")) {
                MaterializedView materializedView;
                try {
                    materializedView = materializedViewService.get(project, node.getSuffix());
                }
                catch (Exception e) {
                    throw new RakamException(String.format("Referenced materialized table %s is not exist", node.getSuffix()), BAD_REQUEST);
                }
                if (fetchReference) {
                    materializedViews.computeIfAbsent(materializedView, (key) -> materializedViewService.lockAndUpdateView(project, materializedView));
                    return "";
                }
                else {
                    return materializedViews.get(materializedView).computeQuery;
                }
            }
            return executor.formatTableReference(project, node, sample);
        };
    }

    public CompletableFuture<List<SchemaField>> metadata(String project, String query)
    {
        StringBuilder builder = new StringBuilder();
        Query queryStatement;
        try {
            queryStatement = (Query) parser.createStatement(checkNotNull(query, "query is required"));
        }
        catch (Exception e) {
            throw new RakamException("Unable to parse query: " + e.getMessage(), BAD_REQUEST);
        }

        new QueryFormatter(builder, qualifiedName -> executor.formatTableReference(project, qualifiedName, Optional.empty()), escapeIdentifier)
                .process(queryStatement, 1);

        QueryExecution execution = executor
                .executeRawQuery(builder.toString() + " limit 0");
        CompletableFuture<List<SchemaField>> f = new CompletableFuture<>();
        execution.getResult().thenAccept(result -> {
            if (result.isFailed()) {
                f.completeExceptionally(new RakamException(result.getError().message, HttpResponseStatus.INTERNAL_SERVER_ERROR));
            }
            else {
                f.complete(result.getMetadata());
            }
        });
        return f;
    }
}
