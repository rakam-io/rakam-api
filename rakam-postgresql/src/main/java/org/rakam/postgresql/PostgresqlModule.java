package org.rakam.postgresql;

import com.google.api.client.util.Throwables;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.EscapeIdentifier;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.JDBCConfig;
import org.rakam.config.MetadataConfig;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.postgresql.analysis.PostgresqlConfig;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.plugin.user.PostgresqlUserService;
import org.rakam.postgresql.plugin.user.PostgresqlUserStorage;
import org.rakam.util.ConditionalModule;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static org.rakam.postgresql.plugin.user.PostgresqlUserService.ANONYMOUS_ID_MAPPING;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

@AutoService(RakamModule.class)
@ConditionalModule(config = "store.adapter", value = "postgresql")
public class PostgresqlModule
        extends RakamModule {
    public synchronized static Module getAsyncClientModule(JDBCConfig config) {
        JDBCConfig asyncClientConfig;
        try {
            final String url = config.getUrl();

            asyncClientConfig = new JDBCConfig()
                    .setPassword(config.getPassword())
                    .setTable(config.getTable())
                    .setMaxConnection(4)
                    .setConnectionMaxLifeTime(0L)
                    .setConnectionIdleTimeout(0L)
                    .setConnectionDisablePool(config.getConnectionDisablePool())
                    .setUrl("jdbc:pgsql" + url.substring("jdbc:postgresql".length()))
                    .setUsername(config.getUsername());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return new AbstractConfigurationAwareModule() {
            @Override
            protected void setup(Binder binder) {
                binder.bind(JDBCPoolDataSource.class)
                        .annotatedWith(Names.named("async-postgresql"))
                        .toProvider(new JDBCPoolDataSourceProvider(asyncClientConfig))
                        .in(Scopes.SINGLETON);
            }
        };
    }

    @Override
    protected void setup(Binder binder) {
        JDBCConfig config = buildConfigObject(JDBCConfig.class, "store.adapter.postgresql");
        PostgresqlConfig postgresqlConfig = buildConfigObject(PostgresqlConfig.class);
        MetadataConfig metadataConfig = buildConfigObject(MetadataConfig.class);

        JDBCPoolDataSource orCreateDataSource = JDBCPoolDataSource.getOrCreateDataSource(config, "set time zone 'UTC'");
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("store.adapter.postgresql"))
                .toInstance(orCreateDataSource);

        binder.bind(char.class).annotatedWith(EscapeIdentifier.class).toInstance('"');

        binder.bind(Metastore.class).to(PostgresqlMetastore.class).asEagerSingleton();
        binder.bind(ApiKeyService.class).toInstance(new PostgresqlApiKeyService(orCreateDataSource));

        binder.bind(PostgresqlVersion.class).asEagerSingleton();

        boolean isUserModulePostgresql = "postgresql".equals(getConfig("plugin.user.storage"));
        if (isUserModulePostgresql) {
            binder.bind(AbstractUserService.class).to(PostgresqlUserService.class)
                    .in(Scopes.SINGLETON);
            binder.bind(PostgresqlUserStorage.class).to(PostgresqlUserStorage.class)
                    .in(Scopes.SINGLETON);
        } else {
            binder.bind(boolean.class).annotatedWith(Names.named("user.storage.postgresql"))
                    .toInstance(false);
        }

        if (metadataConfig.getEventStore() == null) {
            binder.bind(EventStore.class).to(PostgresqlEventStore.class).in(Scopes.SINGLETON);
        }

        // use same jdbc pool if report.metadata.store is not set explicitly.
        if (getConfig("report.metadata.store") == null) {
            binder.bind(JDBCPoolDataSource.class)
                    .annotatedWith(Names.named("report.metadata.store.jdbc"))
                    .toInstance(orCreateDataSource);

            binder.bind(ConfigManager.class).to(PostgresqlConfigManager.class);
        }

        if (postgresqlConfig.isAutoIndexColumns()) {
            binder.bind(CollectionFieldIndexerListener.class).asEagerSingleton();
        }

        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        if (userPluginConfig.getEnableUserMapping()) {
            binder.bind(UserMergeTableHook.class).asEagerSingleton();
        }
    }

    @Override
    public String name() {
        return "Postgresql Module";
    }

    @Override
    public String description() {
        return "Postgresql deployment type module";
    }

    private static class JDBCPoolDataSourceProvider
            implements Provider<JDBCPoolDataSource> {
        private final JDBCConfig asyncClientConfig;

        public JDBCPoolDataSourceProvider(JDBCConfig asyncClientConfig) {
            this.asyncClientConfig = asyncClientConfig;
        }

        @Override
        public JDBCPoolDataSource get() {
            return JDBCPoolDataSource.getOrCreateDataSource(asyncClientConfig);
        }
    }

    public static class PostgresqlVersion {
        private Version version;

        @Inject
        public PostgresqlVersion(@Named("store.adapter.postgresql") JDBCPoolDataSource dataSource) {
            try (Connection conn = dataSource.getConnection()) {
                Statement statement = conn.createStatement();
                ResultSet resultsSet = statement.executeQuery("SHOW server_version");
                resultsSet.next();
                String version = resultsSet.getString(1);
                String[] split = version.split("\\.", 2);

                if (Integer.parseInt(split[0]) > 9) {
                    this.version = Version.PG10;
                } else if (Integer.parseInt(split[0]) == 9 && Double.parseDouble(split[1]) >= 5) {
                    this.version = Version.PG_MIN_9_5;
                } else {
                    this.version = Version.OLD;
                }
            } catch (Exception e) {
                this.version = Version.OLD;
            }
        }

        public Version getVersion() {
            return version;
        }

        public enum Version {
            OLD, PG_MIN_9_5, PG10
        }
    }

    private static class CollectionFieldIndexerListener {
        private final ProjectConfig projectConfig;
        private final JDBCPoolDataSource connectionPool;
        boolean postgresql9_5;
        private Set<FieldType> brinSupportedTypes = ImmutableSet.of(FieldType.DATE, FieldType.DECIMAL,
                FieldType.DOUBLE, FieldType.INTEGER, FieldType.LONG,
                FieldType.STRING, FieldType.TIMESTAMP, FieldType.TIME);

        @Inject
        public CollectionFieldIndexerListener(ProjectConfig projectConfig, @com.google.inject.name.Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool, PostgresqlVersion version) {
            this.connectionPool = connectionPool;
            this.projectConfig = projectConfig;
            // Postgresql BRIN support came in 9.5 version
            postgresql9_5 = version.getVersion() != PostgresqlVersion.Version.OLD;
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
            onCreateCollectionFields(event.project, event.collection, event.fields);
        }

        @Subscribe
        public void onCreateCollectionFields(SystemEvents.CollectionFieldCreatedEvent event) {
            onCreateCollectionFields(event.project, event.collection, event.fields);
        }

        public void onCreateCollectionFields(String project, String collection, List<SchemaField> fields) {
            try (Connection conn = connectionPool.getConnection()) {
                Statement statement = conn.createStatement();
                for (SchemaField field : fields) {
                    try {
                        // We cant't use CONCURRENTLY because it causes dead-lock with ALTER TABLE and it's slow.
                        projectConfig.getTimeColumn();
                        statement.execute(String.format("CREATE INDEX %s %s ON %s.%s USING %s(%s)",
                                postgresql9_5 ? "IF NOT EXISTS" : "",
                                checkCollection(String.format("%s_%s_%s_auto_index", project, collection, field.getName())),
                                project, checkCollection(collection),
                                (postgresql9_5 && field.getName().equals(projectConfig.getTimeColumn())) ? "BRIN" : "BTREE",
                                checkTableColumn(field.getName())));
                    } catch (Exception e) {
                        if (postgresql9_5) {
                            throw e;
                        }
                    }
                }
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }

    }

    public static class UserMergeTableHook {
        private final JDBCPoolDataSource connectionPool;
        private final ProjectConfig projectConfig;

        @Inject
        public UserMergeTableHook(ProjectConfig projectConfig, @com.google.inject.name.Named("store.adapter.postgresql") JDBCPoolDataSource connectionPool) {
            this.projectConfig = projectConfig;
            this.connectionPool = connectionPool;
        }

        @Subscribe
        public void onCreateProject(SystemEvents.ProjectCreatedEvent event) {
            try (Connection conn = connectionPool.getConnection()) {
                String query = format("CREATE TABLE %s.%s(id VARCHAR, %s VARCHAR, created_at TIMESTAMP, merged_at TIMESTAMP)",
                        checkCollection(event.project), ANONYMOUS_ID_MAPPING, checkCollection(projectConfig.getUserColumn()));
                conn.createStatement().execute(query);
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
