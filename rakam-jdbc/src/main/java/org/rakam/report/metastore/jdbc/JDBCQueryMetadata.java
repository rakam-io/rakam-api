package org.rakam.report.metastore.jdbc;

import com.facebook.presto.sql.SqlFormatter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.units.Duration;
import org.rakam.JDBCConfig;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/02/15 18:03.
 */
@Singleton
public class JDBCQueryMetadata implements QueryMetadataStore {
    Handle dao;

    ResultSetMapper<MaterializedView> reportMapper = new ResultSetMapper<MaterializedView>() {
        @Override
        public MaterializedView map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            Long update_interval = r.getLong("update_interval");
            return new MaterializedView(
                    r.getString("project"),
                    r.getString("name"), r.getString("table_name"), r.getString("query"),
                    update_interval!= null ? new Duration(update_interval, TimeUnit.MILLISECONDS) : null,
                    // we can't use nice postgresql features since we also want to support mysql
                    JsonHelper.read(r.getString("options"), JsonNode.class));
        }
    };

    ResultSetMapper<ContinuousQuery> continuousQueryMapper = new ResultSetMapper<ContinuousQuery>() {
        @Override
        public ContinuousQuery map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new ContinuousQuery(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
                    JsonHelper.read(r.getString(5), List.class), JsonHelper.read(r.getString(6), Map.class));
        }
    };

    @Inject
    public JDBCQueryMetadata(@Named("report.metadata.store.jdbc") JDBCConfig config) {

        DBI dbi = new DBI(format(config.getUrl(), config.getUsername(), config.getPassword()),
                config.getUsername(), config.getUsername());
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
                "  table_name TEXT," +
                "  options TEXT," +
                "  PRIMARY KEY (project, name)" +
                "  )")
                .execute();
    }

    @Override
    public void saveMaterializedView(MaterializedView materializedView) {
        dao.createStatement("INSERT INTO materialized_views (project, name, query, options, table_name, update_interval) VALUES (:project, :name, :query, :options, :table_name, :update_interval)")
                .bind("project", materializedView.project)
                .bind("name", materializedView.name)
                .bind("table_name", materializedView.tableName)
                .bind("query", SqlFormatter.formatSql(materializedView.query))
                .bind("update_interval", materializedView.updateInterval!=null ? materializedView.updateInterval.toMillis() : null)
        .bind("options", JsonHelper.encode(materializedView.options, false))
                .execute();
    }

    @Override
    public void createContinuousQuery(ContinuousQuery report) {
        dao.createStatement("INSERT INTO continuous_queries (project, name, table_name, query, collections, options) VALUES (:project, :name, :tableName, :query, :collections, :options)")
                .bind("project", report.project)
                .bind("name", report.name)
                .bind("tableName", report.tableName)
                .bind("query", report.query)
                .bind("collections", JsonHelper.encode(report.collections))
                .bind("options", JsonHelper.encode(report.options))
//                .bind("last_update", new java.sql.Time(Instant.now().toEpochMilli()))
                .execute();
    }

    @Override
    public void deleteContinuousQuery(String project, String name) {
        dao.createStatement("DELETE FROM reports continuous_queries project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).execute();
    }

    @Override
    public List<ContinuousQuery> getContinuousQueries(String project) {

        return dao.createQuery("SELECT project, name, table_name, query, collections, options FROM continuous_queries WHERE project = :project")
                .bind("project", project).map(continuousQueryMapper).list();
    }

    @Override
    public ContinuousQuery getContinuousQuery(String project, String name) {
        return dao.createQuery("SELECT project, name, table_name, query, collections, options FROM continuous_queries WHERE project = :project AND name = :name")
                .bind("project", project).bind("name", name).map(continuousQueryMapper).first();
    }

    @Override
    public void deleteMaterializedView(String project, String name) {
        dao.createStatement("DELETE FROM reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).execute();
    }

    @Override
    public MaterializedView getMaterializedView(String project, String name) {
        return dao.createQuery("SELECT project, name, query, strategy, options from reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).map(reportMapper).first();
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        return dao.createQuery("SELECT project, name, table_name, query, options, update_interval from reports WHERE project = :project")
                .bind("project", project)
                .map(reportMapper).list();
    }

    @Override
    public Map<String, List<ContinuousQuery>> getAllContinuousQueries() {
        Query<ContinuousQuery> map = dao.createQuery("SELECT project, name, table_name, query, collections, options from continuous_queries")
                .map((index, r, ctx) -> {
                    return new ContinuousQuery(
                            r.getString("project"),
                            r.getString("name"), r.getString("table_name"), r.getString("query"),
                            JsonHelper.read(r.getString("collections"), List.class), JsonHelper.read(r.getString("options"), Map.class));
                });
        return map.list().stream().collect(Collectors.groupingBy(k -> k.project));
    }
}