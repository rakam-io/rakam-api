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


public class JDBCReportMetadata {
    private final DBI dbi;

    ResultSetMapper<Report> mapper = (index, r, ctx) -> {
//        update(new Report(r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), JsonHelper.read(r.getString(6).replace("default_value", "defaultValue").replace("type_settings", "typeSettings"), Map.class)));
        Report report = new Report(r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), JsonHelper.read(r.getString(6), Map.class), r.getBoolean(6));
        if(r.getMetaData().getColumnCount() >= 7) {
            report.setPermission(r.getBoolean(7));
        }
        if(r.getMetaData().getColumnCount() == 8) {
            report.setUserId(r.getInt(8));
        }
        return report;
    };

    @Inject
    public JDBCReportMetadata(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
    }

    public List<Report> getReports(Integer userId, String project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT reports.project, reports.slug, reports.category, reports.name, reports.query, reports.options, reports.shared, reports.user_id FROM reports " +
                    " JOIN web_user_project permission ON (permission.user_id = :user AND permission.project = :project)" +
                    " WHERE reports.project = :project AND (permission.is_admin OR reports.shared OR reports.user_id = :user) " +
                    " ORDER BY reports.created_at")
                    .bind("project", project)
                    .bind("user", userId)
                    .map(mapper)
                    .list();
        }
    }

    public void delete(Integer userId, String project, String slug) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM reports WHERE project = :project AND slug = :slug AND user_id = :user AND (SELECT p.is_admin OR p.user_id = r.user_id FROM reports r JOIN web_user_project p ON (p.user_id = :user AND p.project = :project) WHERE r.slug = :slug AND r.user_id = :user AND r.project = :project)")
                    .bind("project", project).bind("slug", slug).bind("user", userId).execute();
        }
    }

    public void save(Integer userId, Report report) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO reports (project, slug, category, name, query, options, user_id) VALUES (:project, :slug, :category, :name, :query, :options, :user)")
                    .bind("project", report.project)
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
                get(null, userId, report.project(), report.slug);
            } catch (NotExistsException ex) {
                throw e;
            }

            throw new AlreadyExistsException(String.format("Report '%s'", report.slug), HttpResponseStatus.BAD_REQUEST);
        }
    }

    public Report get(Integer requestedUserId, int userId, String project, String slug) {
        try (Handle handle = dbi.open()) {
            Report report = handle.createQuery("SELECT r.project, r.slug, r.category, r.name, query, r.options, r.shared, r.user_id FROM reports r " +
                    " JOIN web_user_project permission ON (permission.user_id = :user AND permission.project = :project)" +
                    " WHERE r.project = :project AND r.slug = :slug AND permission.user_id = :user AND" +
                    " (permission.is_admin OR r.shared OR r.user_id = :requestedUser)")
                    .bind("project", project)
                    .bind("user", userId)
                    .bind("requestedUser", requestedUserId)
                    .bind("slug", slug).map(mapper).first();
            if(report == null) {
                throw new NotExistsException("Report", HttpResponseStatus.NOT_FOUND);
            }
            return report;
        }
    }

    public Report update(Integer userId, Report report) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE reports SET name = :name, query = :query, category = :category, options = :options WHERE project = :project AND slug = :slug AND " +
                    "(SELECT p.is_admin OR p.user_id = r.user_id FROM reports r JOIN web_user_project p ON (p.user_id = :user AND p.project = :project) WHERE r.slug = :slug AND r.project = :project)")
                    .bind("project", report.project)
                    .bind("name", report.name)
                    .bind("query", report.query)
                    .bind("category", report.category)
                    .bind("slug", report.slug)
                    .bind("user", userId)
                    .bind("options", JsonHelper.encode(report.options, false))
                    .execute();
            if (execute == 0) {
                throw new RakamException("Report does not exist", HttpResponseStatus.BAD_REQUEST);
            }
        }
        return report;
    }
}
