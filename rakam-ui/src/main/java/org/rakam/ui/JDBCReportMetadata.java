package org.rakam.ui;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.rakam.plugin.JDBCConfig;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 20:20.
 */
public class JDBCReportMetadata {
    private final Handle dao;

    ResultSetMapper<Report> mapper = (index, r, ctx) ->
            new Report(r.getString(1), r.getString(2),r.getString(3), r.getString(4), JsonHelper.read(r.getString(5), Map.class));

    @Inject
    public JDBCReportMetadata(@Named("report.metadata.store.jdbc") JDBCConfig config) {

        DBI dbi = new DBI(format(config.getUrl(), config.getUsername(), config.getPassword()),
                config.getUsername(), config.getPassword());
        dao = dbi.open();
        setup();
    }

    public void setup() {
        dao.createStatement("CREATE TABLE IF NOT EXISTS reports (" +
                "  project VARCHAR(255) NOT NULL," +
                "  slug VARCHAR(255) NOT NULL," +
                "  name VARCHAR(255) NOT NULL," +
                "  query TEXT NOT NULL," +
                "  options TEXT," +
                "  PRIMARY KEY (project, slug)" +
                "  )")
                .execute();
    }

    public List<Report> getReports(String project) {
        return dao.createQuery("SELECT project, slug, name, query, options FROM reports WHERE project = :project")
                .bind("project", project).map(mapper).list();
    }

    public void delete(String project, String name) {
        dao.createStatement("DELETE FROM reports WHERE project = :project AND name = :name")
                .bind("project", project).bind("name", name).execute();
    }

    public void save(Report report) {
        dao.createStatement("INSERT INTO reports (project, slug, name, query, options) VALUES (:project, :slug, :name, :query, :options)")
                .bind("project", report.project)
                .bind("name", report.name)
                .bind("query", report.query)
                .bind("slug", report.slug)
                .bind("options", JsonHelper.encode(report.options, false))
                .execute();
    }

    public Object get(String project, String name) {
        return dao.createQuery("SELECT project, slug, name, query, options FROM reports WHERE project = :project AND name = :name")
                .bind("project", project)
                .bind("name", name).map(mapper).list();
    }
}
