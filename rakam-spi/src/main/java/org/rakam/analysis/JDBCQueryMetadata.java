package org.rakam.analysis;

import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.units.Duration;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/02/15 18:03.
 */
@Singleton
public class JDBCQueryMetadata implements QueryMetadataStore {
    Handle dao;

    ResultSetMapper<MaterializedView> reportMapper = (index, r, ctx) -> {
        Long update_interval = r.getLong("update_interval");
        return new MaterializedView(
                r.getString("project"),
                r.getString("name"), r.getString("table_name"), r.getString("query"),
                update_interval!= null ? new Duration(update_interval, TimeUnit.MILLISECONDS) : null,
                // we can't use nice postgresql features since we also want to support mysql
                JsonHelper.read(r.getString("options"), Map.class));
    };

    ResultSetMapper<ContinuousQuery> continuousQueryMapper = (index, r, ctx) ->
            new ContinuousQuery(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
            JsonHelper.read(r.getString(5), List.class),
            JsonHelper.read(r.getString(6), List.class),
            JsonHelper.read(r.getString(7), Map.class));

    @Inject
    public JDBCQueryMetadata(@Named("report.metadata.store.jdbc") JDBCConfig config) {

        DBI dbi = new DBI(format(config.getUrl(), config.getUsername(), config.getPassword()),
                config.getUsername(), config.getPassword());

        dao = dbi.open();
        setup();
    }

    public void setup() {
        dao.createStatement("CREATE TABLE IF NOT EXISTS materialized_views (" +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  table_name VARCHAR(255) NOT NULL," +
                "  query TEXT NOT NULL," +
                "  update_interval BIGINT," +
                "  last_updated TIMESTAMP," +
                "  options TEXT," +
                "  PRIMARY KEY (project, name)" +
                "  )")
                .execute();
        dao.createStatement("CREATE TABLE IF NOT EXISTS continuous_queries (" +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  query TEXT NOT NULL," +
                // in order to support mysql, we use json string instead of array type.
                "  collections TEXT," +
                "  partition_keys TEXT," +
                "  table_name TEXT," +
                "  options TEXT," +
                "  PRIMARY KEY (project, name)" +
                "  )")
                .execute();
    }

    @Override
    public void createMaterializedView(MaterializedView materializedView) {
        dao.createStatement("INSERT INTO materialized_views (project, name, query, options, table_name, update_interval) VALUES (:project, :name, :query, :options, :table_name, :update_interval)")
                .bind("project", materializedView.project)
                .bind("name", materializedView.name)
                .bind("table_name", materializedView.table_name)
                .bind("query", materializedView.query)
                .bind("update_interval", materializedView.updateInterval!=null ? materializedView.updateInterval.toMillis() : null)
        .bind("options", JsonHelper.encode(materializedView.options, false))
                .execute();
    }

    @Override
    public void updateMaterializedView(String project, String tableName, Instant last_update) {
        dao.createStatement("UPDATE materialized_views SET last_update = :last_update WHERE project = :project AND table_name = :name")
                .bind("project", project)
                .bind("name", tableName)
                .bind("last_update", last_update.toEpochMilli())
                .execute();
    }

    @Override
    public void createContinuousQuery(ContinuousQuery report) {
        dao.createStatement("INSERT INTO continuous_queries (project, name, table_name, query, collections, partition_keys, options) VALUES (:project, :name, :tableName, :query, :collections, :partitionKeys, :options)")
                .bind("project", report.project)
                .bind("name", report.name)
                .bind("tableName", report.tableName)
                .bind("query", report.query)
                .bind("collections", JsonHelper.encode(report.collections))
                .bind("partitionKeys", JsonHelper.encode(report.partitionKeys))
                .bind("options", JsonHelper.encode(report.options))
//                .bind("last_update", new java.sql.Time(Instant.now().toEpochMilli()))
                .execute();
    }

    @Override
    public void deleteContinuousQuery(String project, String tableName) {
        dao.createStatement("DELETE FROM reports continuous_queries project = :project AND table_name = :name")
                .bind("project", project)
                .bind("name", tableName).execute();
    }

    @Override
    public List<ContinuousQuery> getContinuousQueries(String project) {

        return dao.createQuery("SELECT project, name, table_name, query, collections, partition_keys, options FROM continuous_queries WHERE project = :project")
                .bind("project", project).map(continuousQueryMapper).list();
    }

    @Override
    public ContinuousQuery getContinuousQuery(String project, String tableName) {
        return dao.createQuery("SELECT project, name, table_name, query, collections, partition_keys, options FROM continuous_queries WHERE project = :project AND table_name = :name")
                .bind("project", project).bind("name", tableName).map(continuousQueryMapper).first();
    }

    @Override
    public void deleteMaterializedView(String project, String tableName) {
        dao.createStatement("DELETE FROM materialized_views WHERE project = :project AND table_name = :name")
                .bind("project", project)
                .bind("name", tableName).execute();
    }

    @Override
    public MaterializedView getMaterializedView(String project, String tableName) {
        return dao.createQuery("SELECT project, name, query, strategy, options from materialized_views WHERE project = :project AND table_name = :name")
                .bind("project", project)
                .bind("name", tableName).map(reportMapper).first();
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        return dao.createQuery("SELECT project, name, table_name, query, options, update_interval from materialized_views WHERE project = :project")
                .bind("project", project)
                .map(reportMapper).list();
    }

    @Override
    public List<MaterializedView> getAllMaterializedViews() {
        return dao.createQuery("SELECT project, name, table_name, query, options, update_interval from materialized_views")
                .map(reportMapper).list();
    }

    @Override
    public List<ContinuousQuery> getAllContinuousQueries() {
        return dao.createQuery("SELECT project, name, table_name, query, collections, partition_keys, options from continuous_queries")
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