package org.rakam.report;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.*;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.EscapeIdentifier;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.MaterializedViewService.MaterializedViewExecution;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.*;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.report.QueryResult.EXECUTION_TIME;

public class QueryExecutorService {
    private final static Logger LOGGER = Logger.get(QueryExecutorService.class);

    public static final int DEFAULT_QUERY_RESULT_COUNT = 50000;
    public static final int MAX_QUERY_RESULT_LIMIT = 1000000;

    private final QueryExecutor executor;
    private final MaterializedViewService materializedViewService;
    private final Metastore metastore;
    private final char escapeIdentifier;
    private volatile Set<String> projectCache;
    private final ScheduledExecutorService asyncMaterializedViewUpdateQueue;

    @Inject
    public QueryExecutorService(QueryExecutor executor, Metastore metastore, MaterializedViewService materializedViewService, @EscapeIdentifier char escapeIdentifier) {
        this.executor = executor;
        this.materializedViewService = materializedViewService;
        this.metastore = metastore;
        this.escapeIdentifier = escapeIdentifier;
        this.asyncMaterializedViewUpdateQueue = Executors.newScheduledThreadPool(1);
    }

    public QueryExecution executeQuery(String project, String sqlQuery, Optional<QuerySampling> sample, String defaultSchema, ZoneId zoneId, int limit) {
        return executeQuery(new RequestContext(project, null), sqlQuery, sample, defaultSchema, zoneId, limit);
    }

    public QueryExecution executeQuery(String project, String sqlQuery, ZoneId timezone) {
        return executeQuery(new RequestContext(project, null), sqlQuery, timezone);
    }

    public QueryExecution executeQuery(RequestContext context, String sqlQuery, ZoneId timezone) {
        return executeQuery(context, sqlQuery, Optional.empty(), null,
                timezone, DEFAULT_QUERY_RESULT_COUNT);
    }

    public QueryExecution executeQuery(RequestContext context, String sqlQuery, Optional<QuerySampling> sample, String defaultSchema, ZoneId zoneId, int limit) {
        if (!projectExists(context.project)) {
            throw new NotExistsException("Project");
        }
        HashMap<MaterializedView, MaterializedViewExecution> materializedViews = new HashMap<>();
        Map<String, String> sessionParameters = new HashMap<>();

        String query;

        try {
            query = buildQuery(context, sqlQuery, sample, defaultSchema, limit, materializedViews, sessionParameters);
        } catch (ParsingException e) {
            QueryError error = new QueryError(e.getMessage(), null, null, e.getLineNumber(), e.getColumnNumber());
            LogUtil.logQueryError(sqlQuery, error, executor.getClass());
            return QueryExecution.completedQueryExecution(sqlQuery, QueryResult.errorResult(error, sqlQuery));
        }

        long startTime = System.currentTimeMillis();

        List<QueryExecution> syncExecutions = materializedViews.values().stream()
                .filter(e -> e.materializedViewUpdateQuery != null && e.waitForUpdate)
                .map(e -> e.materializedViewUpdateQuery.get()).collect(Collectors.toList());

        Function<QueryResult, QueryResult> attachMaterializedViews = result -> {

            if (!result.isFailed()) {
                Map<String, Long> collect = materializedViews.entrySet().stream()
                        .collect(Collectors.toMap(
                                v -> v.getKey().tableName,
                                v -> Optional.ofNullable(v.getKey().lastUpdate).map(Instant::toEpochMilli).orElse(0L)));
                result.setProperty("materializedViews", collect);
                result.setProperty(EXECUTION_TIME, System.currentTimeMillis() - startTime);
            }

            return result;
        };

        QueryExecution execution;
        if (!syncExecutions.isEmpty()) {
            execution = new ChainQueryExecution(syncExecutions,
                    queryResults -> new DelegateQueryExecution(executor.executeRawQuery(context, query, zoneId, sessionParameters), attachMaterializedViews));
        } else {
            execution = new DelegateQueryExecution(executor.executeRawQuery(context, query, zoneId, sessionParameters), attachMaterializedViews);
        }

        // async execution
        Stream<MaterializedViewExecution> materializedViewExecutionStream = materializedViews.values().stream()
                .filter(e -> e.materializedViewUpdateQuery != null && !e.waitForUpdate);
        materializedViewExecutionStream
                .forEach(e -> {
                    QueryExecution updateQuery = e.materializedViewUpdateQuery.get();
                    updateQuery.getResult().whenComplete((queryResult, throwable) -> {
                        if (throwable != null) {
                            QueryError error = queryResult.getError();
                            String message = format("An exception is thrown while updating materialized table '%s': %s", e.materializedViewUpdateQuery, error.message);
                            LOGGER.error(throwable, message);
                            LogUtil.logQueryError(query, error, executor.getClass());
                        } else if (queryResult.isFailed()) {
                            QueryError error = queryResult.getError();
                            String message = format("Error while updating materialized table '%s': %s", e.materializedViewUpdateQuery, error.message);
                            LOGGER.error(message);
                            LogUtil.logQueryError(query, error, executor.getClass());
                        }
                    });
                });

        return execution;
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

    public String buildQuery(RequestContext context, String query, Optional<QuerySampling> sample, String defaultSchema, Integer maxLimit, Map<MaterializedView, MaterializedViewExecution> materializedViews, Map<String, String> sessionParameters) {
        Query statement;
        Function<QualifiedName, String> tableNameMapper = tableNameMapper(context, materializedViews, sample, defaultSchema, sessionParameters);
        Statement queryStatement = SqlUtil.parseSql(query);
        if ((queryStatement instanceof Query)) {
            statement = (Query) queryStatement;
        } else if ((queryStatement instanceof Call)) {
            StringBuilder builder = new StringBuilder();
            new RakamSqlFormatter.Formatter(builder, tableNameMapper, escapeIdentifier)
                    .process(queryStatement, 1);
            return builder.toString();
        } else {
            throw new RakamException(queryStatement.getClass().getSimpleName() + " is not supported", BAD_REQUEST);
        }

        StringBuilder builder = new StringBuilder();
        new RakamSqlFormatter.Formatter(builder, tableNameMapper, escapeIdentifier)
                .process(statement, 1);

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
            } else {
                builder.append(" LIMIT ").append(maxLimit);
            }
        }

        return builder.toString();
    }

    private Function<QualifiedName, String> tableNameMapper(RequestContext context, Map<MaterializedView, MaterializedViewExecution> materializedViews, Optional<QuerySampling> sample, String defaultSchema, Map<String, String> sessionParameters) {
        return (node) -> {
            if (node.getPrefix().isPresent() && node.getPrefix().get().toString().equals("materialized")) {
                MaterializedView materializedView;
                try {
                    materializedView = materializedViewService.get(context.project, node.getSuffix());
                } catch (NotExistsException e) {
                    throw new MaterializedViewNotExists(node.getSuffix());
                }

                MaterializedViewExecution materializedViewExecution = materializedViews.computeIfAbsent(materializedView,
                        (key) -> materializedViewService.lockAndUpdateView(context, materializedView));

                if (materializedViewExecution == null) {
                    throw new IllegalStateException();
                }

                return materializedViewExecution.computeQuery;
            }

            if (!node.getPrefix().isPresent() && defaultSchema != null) {
                node = QualifiedName.of(defaultSchema, node.getSuffix());
            }

            return executor.formatTableReference(context.project, node, sample, sessionParameters);
        };
    }

    public CompletableFuture<List<SchemaField>> metadata(RequestContext context, String query) {
        StringBuilder builder = new StringBuilder();
        Query queryStatement;
        try {
            queryStatement = (Query) SqlUtil.parseSql(checkNotNull(query, "query is required"));
        } catch (Exception e) {
            throw new RakamException("Unable to parse query: " + e.getMessage(), BAD_REQUEST);
        }

        Map<String, String> map = new HashMap<>();
        new RakamSqlFormatter.Formatter(builder, qualifiedName ->
                executor.formatTableReference(context.project, qualifiedName, Optional.empty(), map), escapeIdentifier)
                .process(queryStatement, 1);

        QueryExecution execution = executor
                .executeRawQuery(context, builder.toString() + " limit 0", map);
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
