package org.rakam.ui;

import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.AlreadyExistsException;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;


public class JDBCReportMetadata {
    private final DBI dbi;

    ResultSetMapper<Report> mapper = (index, r, ctx) -> {
        if(r.getString(6).contains("columnOptions")) {
            System.out.println(r.getString(2));
        }
            return new Report(r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), JsonHelper.read(r.getString(6), Map.class));
    };

    @Inject
    public JDBCReportMetadata(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
        setup();
    }

    public void setup() {
        try(Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS reports (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  slug VARCHAR(255) NOT NULL," +
                    "  category VARCHAR(255)," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  query TEXT NOT NULL," +
                    "  options TEXT," +
                    "  PRIMARY KEY (project, slug)" +
                    "  )")
                    .execute();
        }
    }

    public List<Report> getReports(String project) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT project, slug, category, name, query, options FROM reports WHERE project = :project")
                    .bind("project", project).map(mapper).list();
        }
    }

    public void delete(String project, String name) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM reports WHERE project = :project AND name = :name")
                    .bind("project", project).bind("name", name).execute();
        }
    }

    public void save(Report report) {
        try(Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO reports (project, slug, category, name, query, options) VALUES (:project, :slug, :category, :name, :query, :options)")
                    .bind("project", report.project)
                    .bind("name", report.name)
                    .bind("query", report.query)
                    .bind("slug", report.slug)
                    .bind("category", report.category)
                    .bind("options", JsonHelper.encode(report.options, false))
                    .execute();
        } catch (UnableToExecuteStatementException e) {
            if(get(report.project(), report.slug) != null) {
                throw new AlreadyExistsException(String.format("Report '%s'", report.slug), HttpResponseStatus.BAD_REQUEST);
            } else {
                throw e;
            }
        }
    }

    public Report get(String project, String slug) {
        try(Handle handle = dbi.open()) {
            return handle.createQuery("SELECT project, slug, category, name, query, options FROM reports WHERE project = :project AND slug = :slug")
                    .bind("project", project)
                    .bind("slug", slug).map(mapper).first();
        }
    }

    public Report update(Report report) {
        try(Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE reports SET name = :name, query = :query, category = :category, options = :options WHERE project = :project AND slug = :slug")
                    .bind("project", report.project)
                    .bind("name", report.name)
                    .bind("query", report.query)
                    .bind("category", report.category)
                    .bind("slug", report.slug)
                    .bind("options", JsonHelper.encode(report.options, false))
                    .execute();
            if(execute == 0) {
                throw new RakamException("Report does not exist", HttpResponseStatus.BAD_REQUEST);
            }
        }
        return report;
    }
}
