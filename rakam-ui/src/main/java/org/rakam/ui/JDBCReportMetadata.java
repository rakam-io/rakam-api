package org.rakam.ui;

import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.ui.report.Report;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JsonHelper;
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class JDBCReportMetadata implements ReportMetadata {
    private final DBI dbi;

    private ResultSetMapper<Report> mapper = (index, r, ctx) -> {
        Report report = new Report(r.getString(2), r.getString(3), r.getString(4), r.getString(5), JsonHelper.read(r.getString(6), Map.class), r.getBoolean(6));
        if(r.getMetaData().getColumnCount() >= 7) {
            report.setPermission(r.getBoolean(7));
        }
        if(r.getMetaData().getColumnCount() == 8 && r.getObject(8) != null) {
            report.setUserId(r.getInt(8));
        }
        return report;
    };

    @Inject
    public JDBCReportMetadata(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
    }

    public List<Report> getReports(Integer requestedUserId, int project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT reports.project_id, reports.slug, reports.category, reports.name, reports.query, reports.options, reports.shared, reports.user_id FROM reports " +
                    " WHERE reports.project_id = :project " +
                    " ORDER BY reports.created_at")
                    .bind("project", project)
                    .map(mapper)
                    .list();
        }
    }

    public void delete(Integer userId, int project, String slug) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM reports WHERE project_id = :project AND slug = :slug" +
                    " AND user_id = :user AND (SELECT p.user_id = r.user_id FROM reports r " +
                    " JOIN web_user_project p ON (p.user_id = :user AND p.id = :project) " +
                    " WHERE r.slug = :slug AND r.user_id = :user AND r.project_id = :project)")
                    .bind("project", project)
                    .bind("slug", slug)
                    .bind("user", userId).execute();
        }
    }

    public void save(Integer userId, int project, Report report) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO reports (project_id, slug, category, name, query, options, user_id) VALUES (:project, :slug, :category, :name, :query, :options, :user)")
                    .bind("project", project)
                    .bind("name", report.name)
                    .bind("query", report.query)
                    .bind("slug", report.slug)
                    .bind("user", userId)
                    .bind("category", report.category)
                    .bind("shared", report.shared)
                    .bind("options", JsonHelper.encode(report.options, false))
                    .execute();
        } catch (UnableToExecuteStatementException e) {
            try {
                get(null, project, report.slug);
            } catch (NotExistsException ex) {
                throw e;
            }

            throw new AlreadyExistsException(String.format("Report '%s'", report.slug), BAD_REQUEST);
        }
    }

    public Report get(Integer requestedUserId, int project, String slug) {
        try (Handle handle = dbi.open()) {
            // test this again
            Report report = handle.createQuery("SELECT r.project_id, r.slug, r.category, r.name, query, r.options, r.shared, r.user_id FROM reports r " +
                    " LEFT JOIN web_user_api_key permission ON (permission.project_id = :project)" +
                    " WHERE r.project_id = :project AND r.slug = :slug AND (" +
                    "((permission.master_key IS NOT NULL OR r.shared OR r.user_id = :requestedUser)))")
                    .bind("project", project)
                    .bind("requestedUser", requestedUserId)
                    .bind("slug", slug).map(mapper).first();
            if(report == null) {
                throw new NotExistsException("Report");
            }
            return report;
        }
    }

    public Report update(Integer userId, int project, Report report) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE reports SET name = :name, query = :query, category = :category, options = :options WHERE project_id = :project AND slug = :slug")
                    .bind("project", project)
                    .bind("name", report.name)
                    .bind("query", report.query)
                    .bind("category", report.category)
                    .bind("slug", report.slug)
                    .bind("user", userId)
                    .bind("options", JsonHelper.encode(report.options, false))
                    .execute();
            if (execute == 0) {
                throw new RakamException("Report does not exist", BAD_REQUEST);
            }
        }
        return report;
    }
}
