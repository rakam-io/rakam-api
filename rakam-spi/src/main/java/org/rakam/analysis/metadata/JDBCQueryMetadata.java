package org.rakam.analysis.metadata;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JsonHelper;
import org.rakam.util.NotExistsException;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.LongMapper;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@Singleton
public class JDBCQueryMetadata implements QueryMetadataStore {
    private final DBI dbi;
    private final LoadingCache<ProjectCollection, MaterializedView> materializedViewCache;
    private final Clock clock;

    ResultSetMapper<MaterializedView> materializedViewMapper = (index, r, ctx) -> {
        Long update_interval = r.getLong("update_interval");
        MaterializedView materializedView = new MaterializedView(
                r.getString("project"),
                r.getString("name"), r.getString("table_name"), r.getString("query"),
                update_interval != null ? Duration.ofMillis(update_interval) : null,
                r.getString("incremental_field"),
                r.getString("options") == null ? null : JsonHelper.read(r.getString("options"), Map.class));
        Long last_updated = r.getLong("last_updated");
        if(last_updated != null && last_updated != 0) {
            materializedView.lastUpdate = Instant.ofEpochSecond(last_updated);
        }
        return materializedView;
    };

    ResultSetMapper<ContinuousQuery> continuousQueryMapper = (index, r, ctx) ->
            new ContinuousQuery(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
            JsonHelper.read(r.getString(6), List.class),
            JsonHelper.read(r.getString(7), Map.class));

    @Inject
    public JDBCQueryMetadata(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource, Clock clock) {
        dbi = new DBI(dataSource);
        this.clock = clock;

        materializedViewCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<ProjectCollection, MaterializedView>() {
            @Override
            public MaterializedView load(ProjectCollection key) throws Exception {
                try (Handle handle = dbi.open()) {
                    MaterializedView first = handle.createQuery("SELECT project, name, query, table_name, update_interval, last_updated, incremental_field, options from materialized_views WHERE project = :project AND table_name = :name")
                            .bind("project", key.project)
                            .bind("name", key.collection)
                            .map(materializedViewMapper).first();
                    if(first == null) {
                        throw new NotExistsException("materialized view", HttpResponseStatus.BAD_REQUEST);
                    }
                    return first;
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
                    "  incremental_field TEXT," +
                    "  options TEXT," +
                    "  PRIMARY KEY (project, table_name)" +
                    "  )")
                    .execute();
            handle.createStatement("CREATE TABLE IF NOT EXISTS continuous_queries (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  table_name VARCHAR(255)  NOT NULL," +
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
            try {
                handle.createStatement("INSERT INTO materialized_views (project, name, query, table_name, update_interval, incremental_field, options) VALUES (:project, :name, :query, :table_name, :update_interval, :incremental_field, :options)")
                        .bind("project", materializedView.project)
                        .bind("name", materializedView.name)
                        .bind("table_name", materializedView.tableName)
                        .bind("query", materializedView.query)
                        .bind("update_interval", materializedView.updateInterval != null ? materializedView.updateInterval.toMillis() : null)
                        .bind("incremental_field", materializedView.incrementalField)
                        .bind("options", JsonHelper.encode(materializedView.options))
                        .execute();
            } catch (Exception e) {
                if (getMaterializedView(materializedView.project, materializedView.tableName) != null) {
                    throw new AlreadyExistsException("Materialized view", HttpResponseStatus.BAD_REQUEST);
                }
            }
        }
    }

    @Override
    public boolean updateMaterializedView(MaterializedView view, CompletableFuture<Instant> releaseLock) {

        Handle handle = dbi.open();
        long lastUpdated = handle.createQuery("SELECT last_updated FROM materialized_views WHERE project = :project AND table_name = :table_name FOR UPDATE")
                    .bind("project", view.project)
                    .bind("table_name", view.tableName)
                    .map(LongMapper.FIRST).first();

        view.lastUpdate = Instant.ofEpochSecond(lastUpdated);
        if(!view.needsUpdate(clock)) {
            return false;
        }

        releaseLock.whenComplete((success, ex) -> {
            if(success != null) {
                view.lastUpdate = success;
                long lastUpdate = view.lastUpdate.getEpochSecond();
                handle.createStatement("UPDATE materialized_views SET last_updated = :last_updated WHERE project = :project AND table_name = :table_name")
                        .bind("project", view.project)
                        .bind("table_name", view.tableName)
                        .bind("last_updated", lastUpdate)
                        .execute();
            }

            handle.close();
        });

        return true;
    }

    @Override
    public void createContinuousQuery(ContinuousQuery report) {
        try(Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO continuous_queries (project, name, table_name, query, partition_keys, options) VALUES (:project, :name, :tableName, :query, :partitionKeys, :options)")
                        .bind("project", report.project)
                        .bind("name", report.name)
                        .bind("tableName", report.tableName)
                        .bind("query", report.query)
                        .bind("partitionKeys", JsonHelper.encode(report.partitionKeys))
                        .bind("options", JsonHelper.encode(report.options))
                        .execute();
            } catch (Exception e) {
                if (getContinuousQuery(report.project, report.tableName) != null) {
                    throw new AlreadyExistsException("Continuous query", HttpResponseStatus.BAD_REQUEST);
                }
                throw new RakamException(e.getCause().getMessage(), HttpResponseStatus.BAD_REQUEST);
            }
        }
    }

    @Override
    public void deleteContinuousQuery(String project, String tableName) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM continuous_queries WHERE project = :project AND table_name = :name")
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
        }
    }

    @Override
    public MaterializedView getMaterializedView(String project, String tableName) {
        try {
            return materializedViewCache.get(new ProjectCollection(project, tableName));
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT project, name, query, table_name, update_interval, last_updated, incremental_field, options from materialized_views WHERE project = :project")
                    .bind("project", project).map(materializedViewMapper).list();
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
                                JsonHelper.read(r.getString("partition_keys"), List.class),
                                JsonHelper.read(r.getString("options"), Map.class));
                    }).list();
        }
    }
}