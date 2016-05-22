package org.rakam.analysis;

import com.google.inject.name.Named;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.Locale;
import java.util.function.Function;

public class JDBCConfigManager implements ConfigManager {

    private final DBI dbi;

    @Inject
    public JDBCConfigManager(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
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
    public <T> void setConfig(String project, String configName, T value) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO config (project, name, value) VALUES (:project, :name, :value) ON DUPLICATE KEY UPDATE value = :value;")
                    .bind("project", project)
                    .bind("name", configName.toUpperCase(Locale.ENGLISH))
                    .bind("value", JsonHelper.encode(value)).execute();
        }
    }

    @Override
    public <T> T computeConfig(String project, String configName, Function<T, T> mapper, Class<T> clazz) {
        try (Handle handle = dbi.open()) {
            ResultIterator<T> valueIt = handle.createQuery("SELECT value FROM config WHERE project = :project AND name = :name FOR UPDATE")
                    .bind("project", project)
                    .bind("name", configName.toUpperCase(Locale.ENGLISH)).map((i, resultSet, statementContext) -> {
                        return JsonHelper.read(resultSet.getString(1), clazz);
                    }).iterator();

            boolean exists = valueIt.hasNext();
            T apply = mapper.apply(exists ? valueIt.next() : null);

            if (!exists) {
                handle.createStatement("INSERT INTO config (project, name, value) VALUES (:project, :name, :value)")
                        .bind("project", project)
                        .bind("name", configName.toUpperCase(Locale.ENGLISH))
                        .bind("value", JsonHelper.encode(apply)).execute();
            } else {
                handle.createStatement("UPDATE config SET value = :value WHERE project = :project AND name = :name")
                        .bind("project", project)
                        .bind("name", configName.toUpperCase(Locale.ENGLISH))
                        .bind("value", JsonHelper.encode(apply)).execute();
            }

            return apply;
        }
    }
}
