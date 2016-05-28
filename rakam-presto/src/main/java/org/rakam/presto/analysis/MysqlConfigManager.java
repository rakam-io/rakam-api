package org.rakam.presto.analysis;

import com.google.common.base.Throwables;
import com.google.inject.name.Named;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.Locale;
import java.util.Optional;

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
    public <T> Optional<T> getConfig(String project, String configName, Class<T> clazz) {
        try (Handle handle = dbi.open()) {
            ResultIterator<T> iterator = handle.createQuery("SELECT value FROM config WHERE project = :project AND name = :name")
                    .bind("project", project)
                    .bind("name", configName.toUpperCase(Locale.ENGLISH)).map((i, resultSet, statementContext) -> {
                        return JsonHelper.read(resultSet.getString(1), clazz);
                    }).iterator();
            return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.<T>empty();
        }
    }

    @Override
    public <T> T setConfigOnce(String project, String configName, T value) {
        try (Handle handle = dbi.open()) {
            Optional<T> config = getConfig(project, configName, (Class<T>) value.getClass());

            return config.orElseGet(() -> {
                try {
                    handle.createStatement("INSERT INTO config (project, name, value) VALUES (:project, :name, :value)")
                            .bind("project", project)
                            .bind("name", configName.toUpperCase(Locale.ENGLISH))
                            .bind("value", JsonHelper.encode(value)).execute();
                    return value;
                } catch (Exception e) {
                    // handle race condition
                    return getConfig(project, configName, (Class<T>) value.getClass())
                            .orElseThrow(() -> Throwables.propagate(e));
                }
            });
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
}
