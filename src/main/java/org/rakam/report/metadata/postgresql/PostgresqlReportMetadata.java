package org.rakam.report.metadata.postgresql;

import com.google.common.base.Throwables;
import com.google.inject.Singleton;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.util.Tuple;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/02/15 18:03.
 */
@Singleton
public class PostgresqlReportMetadata implements ReportMetadataStore {
    Handle dao;

    public PostgresqlReportMetadata(PostgresqlMetadataConfig config) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            Throwables.propagate(e);
        }
        DBI dbi = new DBI(format("jdbc:postgresql://%s/%s", config.getHost(), config.getDatabase()),
                config.getUsername(), config.getUsername());
        dao = dbi.open();

        setup();
    }

    public void setup() {
        dao.createStatement("CREATE TABLE IF NOT EXISTS reports (" +
                "  id SERIAL," +
                "  project VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  query VARCHAR(255) NOT NULL," +
                "  UNIQUE (project, name)" +
                "  )")
                .execute();
    }

    @Override
    public void createReport(String project, String name, String query) {
        dao.createStatement("INSERT INTO reports (project, name, query) VALUES (:project, :name, :query)")
                .bind("project", project)
                .bind("name", name)
                .bind("query", query).execute();
    }

    @Override
    public void deleteReport(String project, String name) {
        dao.createStatement("DELETE FROM reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).execute();
    }

    @Override
    public String getReport(String project, String name) {
        return dao.createQuery("SELECT query from reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).map(String.class).first();
    }

    @Override
    public Map<String, String> getReports(String project) {
        return dao.createQuery("SELECT name, query from reports WHERE project = :project")
                .bind("project", project)
                .map((i, resultSet, statementContext) -> new Tuple<>(resultSet.getString(0), resultSet.getString(1)))
                .list().stream().collect(Collectors.toMap(key -> key.v1(), val -> val.v2()));
    }
}

