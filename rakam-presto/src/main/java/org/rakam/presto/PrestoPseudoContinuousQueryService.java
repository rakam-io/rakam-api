package org.rakam.presto;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoPseudoContinuousQueryService  extends ContinuousQueryService
{
    public final static String CONTINUOUS_QUERY_PREFIX = "$view_";

    private final PrestoQueryExecutor executor;
    private final QueryExecutorService service;

    @Inject
    public PrestoPseudoContinuousQueryService(QueryMetadataStore database, QueryExecutorService service, PrestoQueryExecutor executor)
    {
        super(database);
        this.executor = executor;
        this.service = service;
    }

    @Override
    public QueryExecution create(String project, ContinuousQuery report, boolean replayHistoricalData)
    {
        String query = service.buildQuery(project, report.query, Optional.empty(), "collection", null, new HashMap<>(), new HashMap<>());
        String format = String.format("CREATE VIEW %s.%s AS %s", checkProject(project, '"'), checkCollection(CONTINUOUS_QUERY_PREFIX + report.tableName), query);
        return new DelegateQueryExecution(executor.executeRawStatement(format), result -> {
            if (!result.isFailed()) {
                database.createContinuousQuery(project, report);
            }
            else {
                throw new RakamException(result.getError().toString(), BAD_REQUEST);
            }
            return result;
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String name)
    {
        return executor.executeRawStatement(String.format("DROP VIEW %s.%s", checkProject(project, '"'), checkCollection(CONTINUOUS_QUERY_PREFIX + name))).getResult().thenApply(result -> {
            if (!result.isFailed()) {
                database.deleteContinuousQuery(project, name);
            }
            else {
                throw new RakamException(result.getError().toString(), INTERNAL_SERVER_ERROR);
            }
            return true;
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project)
    {
        Stream<Map.Entry<ContinuousQuery, QueryExecution>> continuous = database.getContinuousQueries(project).stream()
                .map(c -> new AbstractMap.SimpleImmutableEntry<>(c, executor.executeRawQuery("SELECT * FROM " +
                        executor.formatTableReference(project, QualifiedName.of("continuous", c.tableName), Optional.empty(), ImmutableMap.of(), "collection") + " limit 0")));
        return continuous
                .collect(Collectors.toMap(entry -> entry.getKey().tableName, entry -> {
                    QueryResult join = entry.getValue().getResult().join();
                    if (join.isFailed()) {
                        return ImmutableList.of();
                    }
                    return join.getMetadata();
                }));
    }

    @Override
    public boolean test(String project, String query)
    {
        ContinuousQuery continuousQuery;
        try {
            continuousQuery = new ContinuousQuery("test", "name",
                    query, ImmutableList.of(), ImmutableMap.of());
        }
        catch (ParsingException | IllegalArgumentException e) {
            throw new RakamException("Query is not valid: " + e.getMessage(), BAD_REQUEST);
        }

        StringBuilder builder = new StringBuilder();
        new RakamSqlFormatter.Formatter(builder, qualifiedName ->
                executor.formatTableReference(project, qualifiedName, Optional.empty(), ImmutableMap.of(), "collection"), '"')
                .process(continuousQuery.getQuery(), 1);

        QueryExecution execution = executor
                .executeRawQuery(builder.toString() + " limit 0");
        QueryResult result = execution.getResult().join();
        if (result.isFailed()) {
            throw new RakamException("Query error: " + result.getError().message, BAD_REQUEST);
        }
        return !result.isFailed();
    }

    @Override
    public QueryExecution refresh(String project, String tableName)
    {
        throw new RakamException(NOT_IMPLEMENTED);
    }
}
