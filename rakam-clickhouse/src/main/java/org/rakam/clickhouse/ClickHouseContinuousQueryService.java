package org.rakam.clickhouse;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.AlreadyExistsException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ClickHouseContinuousQueryService
        extends ContinuousQueryService
{
    @Inject
    public ClickHouseContinuousQueryService(QueryMetadataStore database)
    {
        super(database);
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
        return null;
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
