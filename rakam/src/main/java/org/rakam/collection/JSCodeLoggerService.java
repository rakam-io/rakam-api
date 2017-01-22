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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Path("/javascript-logger")
@Api(value = "/javascript-logger", nickname = "javascript-logs", description = "Javascript code logs", tags = {"collect", "javascript"})
public class JSCodeLoggerService
{
    private final DBI dbi;
    private final boolean postgresOrMysql;

    @Inject
    public JSCodeLoggerService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource)
    {
        try (Connection handle = dataSource.getConnection(true)) {
            if(handle instanceof MySQLConnection) {
                postgresOrMysql = false;
            } else if (handle instanceof PGConnection) {
                postgresOrMysql = true;
            } else {
                throw new RuntimeException("JsCodeLogger service requires Postgresql or Mysql as dependency.");
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setupLogger()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS javascript_logs (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  type VARCHAR(15) NOT NULL," +
                    "  prefix VARCHAR(255) NOT NULL," +
                    "  error TEXT NOT NULL," +
                    "  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                    "  )")
                    .execute();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get logs", authorizations = @Authorization(value = "master_key"))
    @Path("/test")
    public List<LogEntry> getLogs(@Named("project") String project, @ApiParam(value = "start", required = false) Instant start, @ApiParam(value = "end", required = false) Instant end, @ApiParam(value = "prefix") String prefix)
    {
        String sql = "SELECT id, type, error, %s(created_at) FROM javascript_logs WHERE project = :project AND prefix = :prefix";
        if (start != null) {
            sql += " AND created_at > :start";
        }
        if (end != null) {
            sql += " AND created_at < :end";
        }

        sql += " ORDER BY created_at DESC";

        try (Handle handle = dbi.open()) {
            if (postgresOrMysql) {
                sql = String.format(sql, "to_unixtime");
            } else {
                sql = String.format(sql, "unix_timestamp");
            }

            Query<Map<String, Object>> query = handle.createQuery(sql + " LIMIT 100");
            query.bind("project", project);
            query.bind("prefix", prefix);

            if (start != null) {
                query.bind("start", Timestamp.from(start));
            }
            if (end != null) {
                query.bind("end", Timestamp.from(end));
            }

            return query.map((index, r, ctx) -> {
                return new LogEntry(r.getString(1), Level.valueOf(r.getString(2)), r.getString(3), Instant.ofEpochSecond(r.getInt(4)));
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
                handle.createStatement("INSERT INTO javascript_logs (project, id, type, prefix, error, created_at) " +
                        "VALUES (:project, :id, :type, :prefix, :error, :created_at)")
                        .bind("project", project)
                        .bind("id", id)
                        .bind("type", type)
                        .bind("prefix", prefix)
                        .bind("error", value)
                        .bind("created_at", Timestamp.from(Instant.now()))
                        .execute();
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
