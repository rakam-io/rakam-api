package org.rakam.clickhouse;

import com.google.inject.Inject;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;

import java.util.concurrent.CompletableFuture;

public class ClickHouseMaterializedViewService extends MaterializedViewService {
    @Inject
    public ClickHouseMaterializedViewService(QueryMetadataStore database, QueryExecutor queryExecutor) {
        super(database, queryExecutor, '`');
    }

    @Override
    public CompletableFuture<Void> create(String project, MaterializedView materializedView) {
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        return null;
    }

    @Override
    public MaterializedViewExecution lockAndUpdateView(String project, MaterializedView materializedView) {
        return null;
    }
}
