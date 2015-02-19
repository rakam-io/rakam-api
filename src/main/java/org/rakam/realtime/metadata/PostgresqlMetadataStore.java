package org.rakam.realtime.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.rakam.realtime.AggregationType;
import org.rakam.realtime.RealTimeReport;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/02/15 04:02.
 */
public class PostgresqlMetadataStore implements RealtimeReportMetadataStore {
    Handle dao;

    ResultSetMapper<RealTimeReport> reportMapper = new ResultSetMapper<RealTimeReport>() {
        @Override
        public RealTimeReport map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new RealTimeReport(
                    r.getString("project"),
                    r.getString("name"),
                    JsonHelper.read(r.getString("collections"), List.class),
                    AggregationType.get(r.getString("aggregation")),
                    r.getString("filter"),
                    r.getString("measure"),
                    r.getString("dimension"));
        }
    };

    @Inject
    public PostgresqlMetadataStore(@Named("report.metadata.store.postgresql") PostgresqlConfig config) {

        DBI dbi = new DBI(format("jdbc:postgresql://%s/%s", config.getHost(), config.getDatabase()),
                config.getUsername(), config.getUsername());
        dao = dbi.open();
        setup();
    }

    public void setup() {
        dao.createStatement("CREATE TABLE IF NOT EXISTS real_time_reports (" +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  collections TEXT," +
                "  aggregation TEXT NOT NULL," +
                "  filter TEXT," +
                "  measure TEXT," +
                "  dimension TEXT," +
                "  PRIMARY KEY (project, name)" +
                "  )")
                .execute();
    }

    @Override
    public void saveReport(RealTimeReport report) {
        dao.createStatement("INSERT INTO real_time_reports (project, name, query, strategy, options) VALUES (:project, :name, :query, :strategy, :options)")
                .bind("project", report.project)
                .bind("name", report.name)
                .bind("collections", JsonHelper.encode(report.collections, false))
                .bind("aggregation", report.aggregation)
                .bind("filter", report.filter)
                .bind("measure", report.measure)
                .bind("dimension", report.dimension)
                .execute();
    }

    @Override
    public void deleteReport(String project, String name) {
        dao.createStatement("DELETE FROM real_time_reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).execute();
    }

    @Override
    public RealTimeReport getReport(String project, String name) {
        return dao.createQuery("SELECT * from reports WHERE real_time_reports = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).map(reportMapper).first();
    }

    @Override
    public List<RealTimeReport> getReports(String project) {

        return dao.createQuery("SELECT * from real_time_reports WHERE project = :project")
                .bind("project", project)
                .map(reportMapper).list();
    }
}