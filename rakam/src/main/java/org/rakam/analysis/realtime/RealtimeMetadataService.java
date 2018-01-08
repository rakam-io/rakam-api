package org.rakam.analysis.realtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.report.realtime.RealTimeReport;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JsonHelper;
import org.rakam.util.NotExistsException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.List;
import java.util.Set;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class RealtimeMetadataService {
    private final DBI dbi;

    private ResultSetMapper<RealTimeReport> mapper = (index, r, ctx) -> new RealTimeReport(r.getString(1),
            JsonHelper.read(r.getString(2), new TypeReference<List<RealTimeReport.Measure>>() {
            }), r.getString(3),
            JsonHelper.read(r.getString(4), Set.class), r.getString(5),
            JsonHelper.read(r.getString(6), List.class));

    @Inject
    public RealtimeMetadataService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setup() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS realtime_reports (" +
                    "  project VARCHAR(255) NOT NULL,\n" +
                    "  table_name VARCHAR(255) NOT NULL,\n" +
                    "  name VARCHAR(255) NOT NULL,\n" +
                    "  collections TEXT NULL,\n" +
                    "  filter VARCHAR(255) NULL,\n" +
                    "  dimensions TEXT NOT NULL,\n" +
                    "  measures TEXT NOT NULL,\n" +
                    "  created_at TIMESTAMP default current_timestamp NOT NULL," +
                    "  PRIMARY KEY (project, table_name)\n" +
                    "  )").execute();
        }
    }

    public List<RealTimeReport> list(String project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, measures, table_name, collections, filter, dimensions FROM realtime_reports " +
                    " WHERE project = :project ")
                    .bind("project", project)
                    .map(mapper)
                    .list();
        }
    }

    public RealTimeReport get(String project, String tableName) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, measures, table_name, collections, filter, dimensions FROM realtime_reports " +
                    " WHERE project = :project AND table_name = :tableName")
                    .bind("project", project)
                    .bind("tableName", tableName)
                    .map(mapper)
                    .first();
        }
    }

    public void delete(String project, String tableName) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("DELETE FROM realtime_reports WHERE project = :project AND table_name = :table_name")
                    .bind("project", project)
                    .bind("table_name", tableName).execute();
            if (execute == 0) {
                throw new NotExistsException("Report");
            }
        }
    }

    public void save(String project, RealTimeReport report) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO realtime_reports (project, name, table_name, collections, dimensions, filter, measures) " +
                    "VALUES (:project, :name, :table_name, :collections, :dimensions, :filter, :measures)")
                    .bind("project", project)
                    .bind("name", report.name)
                    .bind("table_name", report.table_name)
                    .bind("collections", JsonHelper.encode(report.collections))
                    .bind("dimensions", JsonHelper.encode(report.dimensions))
                    .bind("filter", report.filter)
                    .bind("measures", JsonHelper.encode(report.measures))
                    .execute();
        } catch (UnableToExecuteStatementException e) {
            try {
                list(project).stream().anyMatch(re -> re.table_name.equals(report.table_name));
            } catch (NotExistsException ex) {
                throw e;
            }

            throw new AlreadyExistsException(String.format("Report '%s'", report.table_name), BAD_REQUEST);
        }
    }
}

