package org.rakam.collection;

import com.google.common.base.Throwables;
import com.mysql.jdbc.MySQLConnection;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import io.airlift.log.Level;
import org.postgresql.PGConnection;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Path;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;

@Path("/javascript-logger")
@Api(value = "/javascript-logger", nickname = "javascript-logs", description = "Javascript code logs", tags = "javascript")
public class JSCodeLoggerService
{
    private final DBI dbi;

    @Inject
    public JSCodeLoggerService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource)
    {
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setupLogger()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS javascript_logs (" +
                    "  id VARCHAR(16) NOT NULL," +
                    "  project VARCHAR(255) NOT NULL," +
                    "  type VARCHAR(15) NOT NULL," +
                    "  prefix VARCHAR(255) NOT NULL," +
                    "  error TEXT NOT NULL" +
                    "  )")
                    .execute();
            try {
                handle.createStatement("ALTER TABLE javascript_logs ADD COLUMN created BIGINT NOT NULL DEFAULT 0").execute();
            }
            catch (Exception e) {
                // ignore
            }
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get logs", authorizations = @Authorization(value = "master_key"))
    @Path("/get_logs")
    public List<LogEntry> getLogs(@Named("project") String project, @ApiParam(value = "start", required = false) Instant start, @ApiParam(value = "end", required = false) Instant end, @ApiParam(value = "prefix") String prefix)
    {
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
            query.bind("project", project);
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

    public static class LogEntry
    {
        public final String id;
        public final Level level;
        public final String message;
        public final Instant timestamp;

        public LogEntry(String id, Level level, String message, Instant timestamp)
        {
            this.id = id;
            this.level = level;
            this.message = message;
            this.timestamp = timestamp;
        }
    }

    public PersistentLogger createLogger(String project, String prefix)
    {
        return new PersistentLogger(project, prefix, UUID.randomUUID().toString());
    }

    public PersistentLogger createLogger(String project, String prefix, String identifier)
    {
        return new PersistentLogger(project, prefix, identifier);
    }

    public class PersistentLogger
            implements JSCodeCompiler.ILogger
    {
        private final String prefix;
        private final String project;
        private final String id;

        public PersistentLogger(String project, String prefix, String id)
        {
            this.project = project;
            this.prefix = prefix;
            this.id = id;
        }

        @Override
        public void debug(String value)
        {
            log("DEBUG", value);
        }

        private void log(String type, String value)
        {
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
            }
            catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void warn(String value)
        {
            log("WARN", value);
        }

        @Override
        public void info(String value)
        {
            log("INFO", value);
        }

        @Override
        public void error(String value)
        {
            log("ERROR", value);
        }
    }
}
