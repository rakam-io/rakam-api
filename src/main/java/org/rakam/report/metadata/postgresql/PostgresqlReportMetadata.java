package org.rakam.report.metadata.postgresql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.rakam.analysis.MaterializedView;
import org.rakam.analysis.Report;
import org.rakam.analysis.TableStrategy;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/02/15 18:03.
 */
@Singleton
public class PostgresqlReportMetadata implements ReportMetadataStore {
    Handle dao;

    ResultSetMapper<Report> reportMapper = new ResultSetMapper<Report>() {
        @Override
        public Report map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new Report(
                    r.getString("project"),
                    r.getString("name"), r.getString("query"),
                    JsonHelper.read(r.getString("options"), JsonNode.class));
        }
    };

    ResultSetMapper<MaterializedView> materializedViewMapper = new ResultSetMapper<MaterializedView>() {
        @Override
        public MaterializedView map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            Time last_update = r.getTime("last_update");
            return new MaterializedView(
                    r.getString("project"),
                    r.getString("name"), r.getString("query"),
                    TableStrategy.get(r.getString("strategy")),
                    last_update == null ? null : last_update.toInstant(),
                    Arrays.asList((String[]) r.getArray("collections").getArray()),
                    r.getString("incremental_field"));
        }
    };

    @Inject
    public PostgresqlReportMetadata(@Named("report.metadata.store.postgresql") PostgresqlConfig config) {

        DBI dbi = new DBI(format("jdbc:postgresql://%s/%s", config.getHost(), config.getDatabase()),
                config.getUsername(), config.getUsername());
        dao = dbi.open();
        setup();
    }

    public void setup() {
        dao.createStatement("CREATE TABLE IF NOT EXISTS reports (" +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  query TEXT NOT NULL," +
                "  options TEXT," +
                "  PRIMARY KEY (project, name)" +
                "  )")
                .execute();
        dao.createStatement("CREATE TABLE IF NOT EXISTS materialized_views (" +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  query TEXT NOT NULL," +
                "  strategy TEXT NOT NULL," +
                "  collections TEXT[]," +
                "  last_update TIME," +
                "  incremental_field VARCHAR(255)," +
                "  PRIMARY KEY (project, name)" +
                "  )")
                .execute();
    }

    @Override
    public void saveReport(Report report) {
        dao.createStatement("INSERT INTO reports (project, name, query, options) VALUES (:project, :name, :query, :options)")
                .bind("project", report.project)
                .bind("name", report.name)
                .bind("query", report.query)
                .bind("options", JsonHelper.encode(report.options, false))
                .execute();
    }

    @Override
    public void createMaterializedView(MaterializedView report) {
        dao.createStatement("INSERT INTO materialized_views (project, name, query, strategy, collections, incremental_field) VALUES (:project, :name, :query, :strategy, :collections, :incrementalField)")
                .bind("project", report.project)
                .bind("name", report.name)
                .bind("query", report.query)
                .bind("strategy", report.strategy)
                .bind("collections", report.collections.toArray(new String[report.collections.size()]))
                .bind("incrementalField", report.incrementalField)
                .execute();
    }

    @Override
    public void deleteReport(String project, String name) {
        dao.createStatement("DELETE FROM reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).execute();
    }

    @Override
    public Report getReport(String project, String name) {
        return dao.createQuery("SELECT project, name, query, strategy, options from reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).map(reportMapper).first();
    }

    @Override
    public List<Report> getReports(String project) {
        return dao.createQuery("SELECT project, name, query, options from reports WHERE project = :project")
                .bind("project", project)
                .map(reportMapper).list();
    }

    @Override
    public Map<String, List<MaterializedView>> getAllMaterializedViews(TableStrategy strategy) {
        return dao.createQuery("SELECT project, name, query, strategy, collections, last_update, incremental_field from materialized_views WHERE strategy = :strategy")
                .bind("strategy", strategy.value()).map(materializedViewMapper).list()
                .stream().collect(Collectors.groupingBy(k -> k.project));
    }

    @Override
    public void updateMaterializedView(String project, String viewName, Instant lastUpdate) {
        dao.createStatement("UPDATE materialized_views SET last_update = :lastUpdate WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", viewName).bind("lastUpdate", Timestamp.from(lastUpdate)).execute();
    }
}