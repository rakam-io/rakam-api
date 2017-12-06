package org.rakam.analysis.metadata;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.*;
import org.rakam.util.lock.LockService;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

@Singleton
public class JDBCQueryMetadata
        implements QueryMetadataStore {
    private final DBI dbi;
    private final LoadingCache<ProjectCollection, MaterializedView> materializedViewCache;
    private final LockService lockService;

    private ResultSetMapper<MaterializedView> materializedViewMapper = (index, r, ctx) -> {
        Long update_interval = r.getLong("update_interval");
        MaterializedView materializedView = new MaterializedView(r.getString("table_name"), r.getString("name"), r.getString("query"),
                update_interval != null ? Duration.ofMillis(update_interval) : null,
                r.getBoolean("incremental"),
                r.getBoolean("real_time"),
                r.getString("options") == null ? null : JsonHelper.read(r.getString("options"), Map.class));
        Long last_updated = r.getLong("last_updated");
        if (last_updated != null && last_updated != 0) {
            materializedView.lastUpdate = Instant.ofEpochSecond(last_updated);
        }
        return materializedView;
    };

    @Inject
    public JDBCQueryMetadata(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource, LockService lockService) {
        dbi = new DBI(dataSource);
        this.lockService = lockService;

        materializedViewCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<ProjectCollection, MaterializedView>() {
            @Override
            public MaterializedView load(ProjectCollection key)
                    throws Exception {
                try (Handle handle = dbi.open()) {
                    MaterializedView first = handle.createQuery("SELECT project, name, query, table_name, update_interval, last_updated, incremental, real_time, options " +
                            "from materialized_views WHERE project = :project AND table_name = :name")
                            .bind("project", key.project)
                            .bind("name", key.collection)
                            .map(materializedViewMapper).first();
                    if (first == null) {
                        throw new NotExistsException("Materialized view");
                    }
                    return first;
                }
            }
        });
    }

    @PostConstruct
    public void setup() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS materialized_views (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  real_time BOOLEAN DEFAULT FALSE," +
                    "  table_name VARCHAR(255) NOT NULL," +
                    "  query TEXT NOT NULL," +
                    "  update_interval BIGINT," +
                    "  last_updated BIGINT," +
                    "  incremental BOOLEAN," +
                    "  options TEXT," +
                    "  PRIMARY KEY (project, table_name)" +
                    "  )")
                    .execute();
        }
    }

    @Override
    public void createMaterializedView(String project, MaterializedView materializedView) {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO materialized_views (project, name, query, table_name, update_interval, incremental, real_time, options) " +
                        "VALUES (:project, :name, :query, :table_name, :update_interval, :incremental, :real_time, :options)")
                        .bind("project", project)
                        .bind("name", materializedView.name)
                        .bind("table_name", materializedView.tableName)
                        .bind("query", materializedView.query)
                        .bind("update_interval", materializedView.updateInterval != null ? materializedView.updateInterval.toMillis() : null)
                        .bind("incremental", materializedView.incremental)
                        .bind("real_time", materializedView.realTime)
                        .bind("options", JsonHelper.encode(materializedView.options))
                        .execute();
            } catch (Exception e) {
                try {
                    getMaterializedView(project, materializedView.tableName);
                } catch (RakamException e1) {
                    if (e1.getStatusCode() == NOT_FOUND) {
                        throw Throwables.propagate(e1);
                    }
                }

                throw new AlreadyExistsException("Materialized view", BAD_REQUEST);
            }
        }
    }

    @Override
    public boolean updateMaterializedView(String project, MaterializedView view, CompletableFuture<Instant> releaseLock) {
        LockService.Lock lock = lockService.tryLock(String.format("materialized.%s.%s", project, view.tableName));

        if (lock == null) {
            return false;
        }

        releaseLock.whenComplete((success, ex) -> {
            try {
                if (success != null) {
                    view.lastUpdate = success;
                    try (Handle handle = dbi.open()) {
                        handle.createStatement("UPDATE materialized_views SET last_updated = :last_updated " +
                                "WHERE project = :project AND table_name = :table_name")
                                .bind("project", project)
                                .bind("table_name", view.tableName)
                                .bind("last_updated", success.getEpochSecond())
                                .execute();
                    }
                }
            } finally {
                lock.release();
            }
        });

        return true;
    }

    @Override
    public void changeMaterializedView(String project, String tableName, boolean realTime) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE materialized_views SET real_time = :real_time " +
                    "WHERE project = :project AND table_name = :table_name")
                    .bind("project", project)
                    .bind("table_name", tableName)
                    .bind("real_time", realTime)
                    .execute();
            if (execute == 0) {
                throw new NotExistsException("Materialized view");
            }
        }
    }

    @Override
    public void alter(String project, MaterializedView view) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE materialized_views SET query = :query, table_name = :table_name, update_interval = :update_interval, incremental = :incremental, real_time = :real_time, options = :options " +
                    "WHERE project = :project AND table_name = :table_name")
                    .bind("project", project)
                    .bind("name", view.name)
                    .bind("table_name", view.tableName)
                    .bind("query", view.query)
                    .bind("update_interval", view.updateInterval != null ? view.updateInterval.toMillis() : null)
                    .bind("incremental", view.incremental)
                    .bind("real_time", view.realTime)
                    .bind("options", JsonHelper.encode(view.options))
                    .execute();

            if (execute == 0) {
                throw new NotExistsException("Materialized view");
            }
        }
    }

    @Override
    public void deleteMaterializedView(String project, String tableName) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM materialized_views WHERE project = :project AND table_name = :name")
                    .bind("project", project)
                    .bind("name", tableName).execute();
        }
    }

    @Override
    public MaterializedView getMaterializedView(String project, String tableName) {
        try {
            return materializedViewCache.get(new ProjectCollection(project, tableName));
        } catch (Exception e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, query, table_name, update_interval, last_updated, incremental, real_time, options " +
                    "from materialized_views WHERE project = :project")
                    .bind("project", project).map(materializedViewMapper).list();
        }
    }
}