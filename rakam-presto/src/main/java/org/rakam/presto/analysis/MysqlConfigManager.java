package org.rakam.presto.analysis;

import com.google.common.base.Throwables;
import com.google.inject.name.Named;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Function;

public class MysqlConfigManager implements ConfigManager {

    private final DBI dbi;

    @Inject
    public MysqlConfigManager(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setup() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS config (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  value TEXT," +
                    "  PRIMARY KEY (project, name)" +
                    "  )")
                    .execute();
        }
    }

    @Override
    public <T> T getConfig(String project, String configName, Class<T> clazz) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT value FROM config WHERE project = :project AND name = :name")
                    .bind("project", project)
                    .bind("name", configName.toUpperCase(Locale.ENGLISH)).map((i, resultSet, statementContext) -> {
                        return JsonHelper.read(resultSet.getString(1), clazz);
                    }).first();
        }
    }

    @Override
    public <T> T setConfigOnce(String project, String configName, T value) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO config (project, name, value) VALUES (:project, :name, :value)")
                    .bind("project", project)
                    .bind("name", configName.toUpperCase(Locale.ENGLISH))
                    .bind("value", JsonHelper.encode(value)).execute();
            return null;
        }
    }

    @Override
    public <T> void setConfig(String project, String configName, T value) {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO config (project, name, value) VALUES (:project, :name, :value)")
                        .bind("project", project)
                        .bind("name", configName.toUpperCase(Locale.ENGLISH))
                        .bind("value", JsonHelper.encode(value)).execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public <T> T computeConfig(String project, String configName, Function<T, T> mapper, Class<T> clazz) {
        try (Connection conn = dbi.open().getConnection()) {
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM config WHERE project = ? AND name = ? FOR UPDATE",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE);

            ps.setString(1, project);
            ps.setString(2, configName.toUpperCase(Locale.ENGLISH));

            ResultSet rs = ps.executeQuery();
            boolean exists = rs.next();

            T apply = mapper.apply(exists ? JsonHelper.read(rs.getString("value"), clazz) : null);

            if (!exists) {
                ps = conn.prepareStatement("INSERT INTO config (project, name, value) VALUES (?, ?, ?)");
                ps.setString(1, project);
                ps.setString(2, configName.toUpperCase(Locale.ENGLISH));
                ps.setString(3, JsonHelper.encode(apply));
                try {
                    ps.executeUpdate();
                } catch (SQLException e) {
                    // SELECT FOR UPDATE DOESN'T WORK?!!=
                }
            } else {
                rs.updateString("value", JsonHelper.encode(apply));
                rs.updateRow();
            }

            return apply;
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
