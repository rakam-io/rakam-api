package org.rakam.plugin;

import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.RakamException;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static java.lang.String.format;


public abstract class MaterializedViewService {
    private final QueryMetadataStore database;
    private final QueryExecutor queryExecutor;
    private final Clock clock;

    public MaterializedViewService(QueryExecutor queryExecutor, QueryMetadataStore database, Clock clock) {
        this.database = database;
        this.queryExecutor = queryExecutor;
        this.clock = clock;
    }

    public CompletableFuture<Void> create(MaterializedView materializedView) {
        materializedView.validateQuery();
        QueryResult result = queryExecutor.executeStatement(materializedView.project, format("CREATE TABLE materialized.%s AS (%s LIMIT 0)",
                materializedView.table_name, materializedView.query)).getResult().join();
        if(result.isFailed()) {
            throw new RakamException("Couldn't created table: "+result.getError().toString(), UNAUTHORIZED);
        }
        database.createMaterializedView(materializedView);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        return queryExecutor.executeStatement(project, format("DELETE TABLE materialized.%s", materializedView.table_name)).getResult();
    }
    public List<MaterializedView> list(String project) {
        return database.getMaterializedViews(project);
    }

    public MaterializedView get(String project, String name) {
        return database.getMaterializedView(project, name);
    }

    public abstract Map<String, List<SchemaField>> getSchemas(String project);

    public QueryExecution update(MaterializedView materializedView) {
        if(materializedView.lastUpdate!=null) {
            QueryResult result = queryExecutor.executeStatement(materializedView.project,
                    format("DROP TABLE materialized.%s", materializedView.table_name)).getResult().join();
            if(result.isFailed()) {
                return new QueryExecution() {
                    @Override
                    public QueryStats currentStats() {
                        return null;
                    }

                    @Override
                    public boolean isFinished() {
                        return true;
                    }

                    @Override
                    public CompletableFuture<QueryResult> getResult() {
                        return CompletableFuture.completedFuture(result);
                    }

                    @Override
                    public String getQuery() {
                        return null;
                    }

                    @Override
                    public void kill() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }
        QueryExecution queryExecution = queryExecutor.executeStatement(materializedView.project, format("CREATE TABLE materialized.%s AS (%s)",
                materializedView.table_name, materializedView.query));

        queryExecution.getResult().thenAccept(result -> {
            if(!result.isFailed()) {
                database.updateMaterializedView(materializedView.project, materializedView.name, clock.instant());
            }
        });

        return queryExecution;
    }}
