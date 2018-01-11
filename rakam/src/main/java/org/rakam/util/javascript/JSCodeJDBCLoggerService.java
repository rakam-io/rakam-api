package org.rakam.util.javascript;

import com.google.common.base.Throwables;
import io.airlift.log.Level;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.RequestContext;
import org.rakam.server.http.annotations.*;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Path("/javascript-logger")
@Api(value = "/javascript-logger", nickname = "javascript-logs", description = "Javascript code logs", tags = "javascript")
public class JSCodeJDBCLoggerService implements JSLoggerService {
    private final DBI dbi;

    @Inject
    public JSCodeJDBCLoggerService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setupLogger() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS javascript_logs (" +
                    "  id TEXT NOT NULL," +
                    "  project VARCHAR(255) NOT NULL," +
                    "  type VARCHAR(15) NOT NULL," +
                    "  prefix VARCHAR(255) NOT NULL," +
                    "  error TEXT NOT NULL" +
                    "  )")
                    .execute();
            try {
                handle.createStatement("ALTER TABLE javascript_logs ADD COLUMN created BIGINT NOT NULL DEFAULT 0").execute();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get logs", authorizations = @Authorization(value = "master_key"))
    @Path("/get_logs")
    public List<LogEntry> getLogs(@Named("project") RequestContext context, @ApiParam(value = "start", required = false) Instant start, @ApiParam(value = "end", required = false) Instant end, @ApiParam(value = "prefix") String prefix) {
        String sql = "SELECT id, type, error, created FROM javascript_logs WHERE project = :project AND prefix = :prefix";
        if (start != null) {
            sql += " AND created > :start";
        }
        if (end != null) {
            sql += " AND created < :end";
        }

        sql += " ORDER BY created DESC";

        try (Handle handle = dbi.open()) {
            Query<Map<String, Object>> query = handle.createQuery(sql + " LIMIT 100");
            query.bind("project", context.project);
            query.bind("prefix", prefix);

            if (start != null) {
                query.bind("start", start.toEpochMilli());
            }
            if (end != null) {
                query.bind("end", end.toEpochMilli());
            }

            return query.map((index, r, ctx) -> {
                return new LogEntry(r.getString(1), Level.valueOf(r.getString(2)), r.getString(3), Instant.ofEpochMilli(r.getLong(4)));
            }).list();
        }
    }


    public PersistentLogger createLogger(String project, String prefix) {
        return new PersistentLogger(project, prefix, UUID.randomUUID().toString());
    }

    public PersistentLogger createLogger(String project, String prefix, String identifier) {
        return new PersistentLogger(project, prefix, identifier);
    }

    public class PersistentLogger
            implements ILogger {
        private final String prefix;
        private final String project;
        private final String id;

        public PersistentLogger(String project, String prefix, String id) {
            this.project = project;
            this.prefix = prefix;
            this.id = id;
        }

        @Override
        public void debug(String value) {
            log("DEBUG", value);
        }

        private void log(String type, String value) {
            try (Handle handle = dbi.open()) {
                PreparedStatement preparedStatement = handle.getConnection().prepareStatement(
                        "INSERT INTO javascript_logs (project, id, type, prefix, error, created) " +
                                "VALUES (?, ?, ?, ?, ?, ?)");
                preparedStatement.setString(1, project);
                preparedStatement.setString(2, id);
                preparedStatement.setString(3, type);
                preparedStatement.setString(4, prefix);
                preparedStatement.setString(5, value);
                preparedStatement.setLong(6, Instant.now().toEpochMilli());
                preparedStatement.execute();
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void warn(String value) {
            log("WARN", value);
        }

        @Override
        public void info(String value) {
            log("INFO", value);
        }

        @Override
        public void error(String value) {
            log("ERROR", value);
        }
    }
}
