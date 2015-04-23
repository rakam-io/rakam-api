package org.rakam.plugin;

import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.RakamException;
import org.rakam.util.Tuple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 05:30.
 */
public abstract class MaterializedViewService {
    private final QueryMetadataStore database;
    private final EventSchemaMetastore metastore;
    private final QueryExecutor queryExecutor;

    @Inject
    public MaterializedViewService(QueryExecutor queryExecutor, QueryMetadataStore database, EventSchemaMetastore metastore) {
        this.queryExecutor = queryExecutor;
        this.database = database;
        this.metastore = metastore;

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public void create(MaterializedView materializedView) {
        QueryResult result = queryExecutor.executeStatement(materializedView.project, format("CREATE TABLE materialized.%s AS (%s LIMIT 0)",
                materializedView.tableName, materializedView.query)).getResult().join();
        if(result.isFailed()) {
            throw new RakamException("Couldn't created table: "+result.getError().toString(), 400);
        }
        database.saveMaterializedView(materializedView);
    }

    public List<MaterializedView> list(String project) {
        return database.getMaterializedViews(project);
    }

    public CompletableFuture<? extends QueryResult> delete(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        database.deleteMaterializedView(project, name);
        return queryExecutor.executeStatement(project, format("DELETE TABLE materialized.%s", materializedView.tableName)).getResult();
    }

    public MaterializedView get(String project, String name) {
        return database.getMaterializedView(project, name);
    }

    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new Tuple<>(view.name, metastore.getSchema(project, view.tableName)))
                .collect(Collectors.toMap(t -> t.v1(), t -> t.v2()));
    }

    public QueryExecution update(String project, String name) {
        MaterializedView materializedView = database.getMaterializedView(project, name);
        if(materializedView.lastUpdate!=null) {
            QueryResult result = queryExecutor.executeStatement(project, format("DROP TABLE materialized.%s", materializedView.tableName)).getResult().join();
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
                    public CompletableFuture<? extends QueryResult> getResult() {
                        return CompletableFuture.completedFuture(result);
                    }

                    @Override
                    public String getQuery() {
                        return null;
                    }
                };
            }
        }
        return queryExecutor.executeStatement(materializedView.project, format("CREATE TABLE materialized.%s AS (%s)",
                materializedView.tableName, materializedView.query));
    }
}
