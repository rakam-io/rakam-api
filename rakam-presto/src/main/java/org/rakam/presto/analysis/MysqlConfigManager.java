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
import java.util.Locale;

public class MysqlConfigManager
        implements ConfigManager {

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
            T config = getConfig(project, configName, (Class<T>) value.getClass());

            if (config == null) {
                try {
                    handle.createStatement("INSERT INTO config (project, name, value) VALUES (:project, :name, :value)")
                            .bind("project", project)
                            .bind("name", configName.toUpperCase(Locale.ENGLISH))
                            .bind("value", JsonHelper.encode(value)).execute();
                    return value;
                } catch (Exception e) {
                    // handle race condition
                    T lastValue = getConfig(project, configName, (Class<T>) value.getClass());
                    if (lastValue == null) {
                        throw Throwables.propagate(e);
                    }
                    return lastValue;
                }
            } else {
                return config;
            }
        }
    }

    @Override
    public void clear() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM config").execute();
        }
    }

    @Override
    public <T> void setConfig(String project, String configName, T value) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO config (project, name, value) VALUES (:project, :name, :value) ON DUPLICATE KEY UPDATE value = :value")
                    .bind("project", project)
                    .bind("name", configName.toUpperCase(Locale.ENGLISH))
                    .bind("value", JsonHelper.encode(value)).execute();
        }
    }
}
