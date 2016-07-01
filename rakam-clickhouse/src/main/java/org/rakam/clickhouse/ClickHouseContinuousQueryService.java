package org.rakam.clickhouse;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

public class ClickHouseContinuousQueryService
        extends ContinuousQueryService
{
    private final QueryExecutor queryExecutor;

    @Inject
    public ClickHouseContinuousQueryService(QueryExecutor queryExecutor, QueryMetadataStore database)
    {
        super(database);
        this.queryExecutor = queryExecutor;
    }

    @Override
    public QueryExecution create(String project, ContinuousQuery report, boolean replayHistoricalData)
            throws AlreadyExistsException
    {
        database.createContinuousQuery(project, report);
        return QueryExecution.completedQueryExecution(null, QueryResult.empty());
    }

    @Override
    public CompletableFuture<Boolean> delete(String project, String tableName)
    {
        String prestoQuery = format("drop table %s.%s", project, ValidationUtil.checkCollection(tableName));
        return queryExecutor.executeRawQuery(prestoQuery).getResult().thenApply(result -> {
            if (result.getError() == null) {
                database.deleteContinuousQuery(project, tableName);
                return true;
            } else {
                throw new RakamException("Error while deleting continuous query:" + result.getError().message, BAD_REQUEST);
            }
        });
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project)
    {
        return ImmutableMap.of();
    }

    @Override
    public boolean test(String project, String query)
    {
        return false;
    }

    @Override
    public QueryExecution refresh(String project, String tableName)
    {
        return null;
    }
}
