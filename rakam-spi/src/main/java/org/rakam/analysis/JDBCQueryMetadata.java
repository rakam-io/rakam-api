package org.rakam.analysis;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.JsonHelper;
import org.rakam.util.ProjectCollection;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


@Singleton
public class JDBCQueryMetadata implements QueryMetadataStore {
    private final DBI dbi;
    private final LoadingCache<ProjectCollection, MaterializedView> materializedViewCache;

    ResultSetMapper<MaterializedView> reportMapper = (index, r, ctx) -> {
        Long update_interval = r.getLong("update_interval");
        MaterializedView materializedView = new MaterializedView(
                r.getString("project"),
                r.getString("name"), r.getString("table_name"), r.getString("query"),
                update_interval != null ? Duration.ofMillis(update_interval) : null,
                // we can't use nice postgresql features since we also want to support mysql
                JsonHelper.read(r.getString("options"), Map.class));
        long last_updated = r.getLong("last_updated");
        if(last_updated != 0) {
            materializedView.lastUpdate = Instant.ofEpochMilli(last_updated);
        }
        return materializedView;
    };

    ResultSetMapper<ContinuousQuery> continuousQueryMapper = (index, r, ctx) ->
            new ContinuousQuery(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
            JsonHelper.read(r.getString(5), List.class),
            JsonHelper.read(r.getString(6), List.class),
            JsonHelper.read(r.getString(7), Map.class));

    @Inject
    public JDBCQueryMetadata(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);

        materializedViewCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<ProjectCollection, MaterializedView>() {
            @Override
            public MaterializedView load(ProjectCollection key) throws Exception {
                try (Handle handle = dbi.open()) {
                    return handle.createQuery("SELECT project, name, query, table_name, update_interval, last_updated, options from materialized_views WHERE project = :project AND table_name = :name")
                            .bind("project", key.project)
                            .bind("name", key.collection).map(reportMapper).first();
                }
            }
        });
        setup();
    }

    public void setup() {
        try(Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS materialized_views (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  table_name VARCHAR(255) NOT NULL," +
                    "  query TEXT NOT NULL," +
                    "  update_interval BIGINT," +
                    "  last_updated BIGINT," +
                    "  options TEXT," +
                    "  PRIMARY KEY (project, table_name)" +
                    "  )")
                    .execute();
            handle.createStatement("CREATE TABLE IF NOT EXISTS continuous_queries (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  table_name TEXT," +
                    "  query TEXT NOT NULL," +
                    // in order to support mysql, we use json string instead of array type.
                    "  collections TEXT," +
                    "  partition_keys TEXT," +
                    "  options TEXT," +
                    "  PRIMARY KEY (project, table_name)" +
                    "  )")
                    .execute();
        }
    }

    @Override
    public void createMaterializedView(MaterializedView materializedView) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO materialized_views (project, name, query, options, table_name, update_interval) VALUES (:project, :name, :query, :options, :table_name, :update_interval)")
                    .bind("project", materializedView.project)
                    .bind("name", materializedView.name)
                    .bind("table_name", materializedView.tableName)
                    .bind("query", materializedView.query)
                    .bind("update_interval", materializedView.updateInterval!=null ? materializedView.updateInterval.toMillis() : null)
                    .bind("options", JsonHelper.encode(materializedView.options, false))
                    .execute();
            materializedViewCache.refresh(new ProjectCollection(materializedView.project, materializedView.tableName));
        }
    }

    @Override
    public boolean updateMaterializedView(MaterializedView view, CompletableFuture<Boolean> releaseLock) {
        int rows;
        try(Handle handle = dbi.open()) {
            rows = handle.createStatement("UPDATE materialized_views SET last_updated = -1 WHERE project = :project AND table_name = :table_name AND (last_updated IS NULL OR last_updated != -1)")
                    .bind("project", view.project)
                    .bind("table_name", view.tableName)
                    .execute();
        }

        releaseLock.thenAccept((success) -> {
            if(success) {
                view.lastUpdate = Instant.now();
            }
            try(Handle handle = dbi.open()) {
                handle.createStatement("UPDATE materialized_views SET last_updated = :last_updated WHERE project = :project AND table_name = :table_name")
                        .bind("project", view.project)
                        .bind("table_name", view.tableName)
                        .bind("last_updated", view.lastUpdate.toEpochMilli())
                        .execute();
            }
        });

        return rows == 1;
    }

    @Override
    public void createContinuousQuery(ContinuousQuery report) {
        try(Handle handle = dbi.open()) {
            StringBuilder builder = new StringBuilder();
            new RakamSqlFormatter.Formatter(builder).process(report.query, 1);
            handle.createStatement("INSERT INTO continuous_queries (project, name, table_name, query, collections, partition_keys, options) VALUES (:project, :name, :tableName, :query, :collections, :partitionKeys, :options)")
                    .bind("project", report.project)
                    .bind("name", report.name)
                    .bind("tableName", report.tableName)
                    .bind("query", builder.toString())
                    .bind("collections", JsonHelper.encode(report.collections))
                    .bind("partitionKeys", JsonHelper.encode(report.partitionKeys))
                    .bind("options", JsonHelper.encode(report.options))
                    .execute();
        }
    }

    @Override
    public void deleteContinuousQuery(String project, String tableName) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM reports WHERE continuous_queries project = :project AND table_name = :name")
                    .bind("project", project)
                    .bind("name", tableName).execute();
        }
    }

    @Override
    public List<ContinuousQuery> getContinuousQueries(String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT project, name, table_name, query, collections, partition_keys, options FROM continuous_queries WHERE project = :project")
                    .bind("project", project).map(continuousQueryMapper).list();
        }
    }

    @Override
    public ContinuousQuery getContinuousQuery(String project, String tableName) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT project, name, table_name, query, collections, partition_keys, options FROM continuous_queries WHERE project = :project AND table_name = :name")
                    .bind("project", project).bind("name", tableName).map(continuousQueryMapper).first();
        }
    }

    @Override
    public void deleteMaterializedView(String project, String tableName) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM materialized_views WHERE project = :project AND table_name = :name")
                    .bind("project", project)
                    .bind("name", tableName).execute();
            materializedViewCache.refresh(new ProjectCollection(project, tableName));
        }
    }

    @Override
    public MaterializedView getMaterializedView(String project, String tableName) {
        materializedViewCache.refresh(new ProjectCollection(project, tableName));
        return materializedViewCache.getUnchecked(new ProjectCollection(project, tableName));
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT project, name, query, table_name, update_interval, last_updated, options from materialized_views WHERE project = :project")
                    .bind("project", project).map(reportMapper).list();
        }
    }

    @Override
    public List<ContinuousQuery> getAllContinuousQueries() {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT project, name, table_name, query, collections, partition_keys, options from continuous_queries")
                    .map((index, r, ctx) -> {
                        return new ContinuousQuery(
                                r.getString("project"),
                                r.getString("name"), r.getString("table_name"), r.getString("query"),
                                JsonHelper.read(r.getString("collections"), List.class),
                                JsonHelper.read(r.getString("partition_keys"), List.class),
                                JsonHelper.read(r.getString("options"), Map.class));
                    }).list();
        }
    }
}