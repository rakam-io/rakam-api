package org.rakam.analysis.postgresql;

import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Clock;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static java.lang.String.format;

public class PostgresqlMaterializedViewService extends MaterializedViewService {
    private final PostgresqlMetastore metastore;
    private final PostgresqlQueryExecutor queryExecutor;

    @Inject
    public PostgresqlMaterializedViewService(PostgresqlQueryExecutor queryExecutor, QueryMetadataStore database, PostgresqlMetastore metastore, Clock clock) {
        super(queryExecutor, database, clock);
        this.metastore = metastore;
        this.queryExecutor = queryExecutor;

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            for (MaterializedView materializedView : database.getAllMaterializedViews()) {
                long gap = clock.millis() - materializedView.lastUpdate.toEpochMilli();
                if (gap > materializedView.updateInterval.toMillis()) {
                    update(materializedView);
                }
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    @Override
    public CompletableFuture<Void> create(MaterializedView materializedView) {
        materializedView.validateQuery();
        String query = queryExecutor.buildQuery(materializedView.project, materializedView.query, null);
        QueryResult result = queryExecutor.executeRawStatement(format("CREATE MATERIALIZED VIEW %s.%s%s AS %s WITH NO DATA",
                materializedView.project, PostgresqlQueryExecutor.MATERIALIZED_VIEW_PREFIX, materializedView.table_name, query)).getResult().join();
        if(result.isFailed()) {
            throw new RakamException("Couldn't created table: "+result.getError().toString(), UNAUTHORIZED);
        }
        database.createMaterializedView(materializedView);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        return queryExecutor.executeRawStatement(format("DROP MATERIALIZED VIEW %s.%s%s",
                materializedView.project, PostgresqlQueryExecutor.MATERIALIZED_VIEW_PREFIX, materializedView.table_name)).getResult();
    }

    @Override
    public QueryExecution update(MaterializedView materializedView) {
        QueryExecution execution = queryExecutor.executeRawStatement(format("REFRESH MATERIALIZED VIEW %s.%s", materializedView.project, materializedView.table_name));
        execution.getResult().thenAccept(result -> {
            if(!result.isFailed()) {
                database.updateMaterializedView(materializedView.project, materializedView.name, materializedView.lastUpdate);
            }
        });
        return execution;
    }

    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new SimpleImmutableEntry<>(view.table_name, metastore.getCollection(project, queryExecutor.MATERIALIZED_VIEW_PREFIX + view.table_name)))
                .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue()));
    }
}
