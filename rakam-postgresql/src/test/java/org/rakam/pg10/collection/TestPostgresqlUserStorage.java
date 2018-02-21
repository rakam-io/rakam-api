package org.rakam.pg10.collection;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.TestUserStorage;
import org.rakam.config.ProjectConfig;
import org.rakam.pg10.TestingEnvironmentPg10;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.postgresql.PostgresqlConfigManager;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.plugin.user.PostgresqlUserService;
import org.rakam.postgresql.plugin.user.PostgresqlUserStorage;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPostgresqlUserStorage
        extends TestUserStorage {
    private TestingEnvironmentPg10 testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlUserService userService;
    private PostgresqlConfigManager configManager;

    @BeforeSuite
    @Override
    public void setUp()
            throws Exception {
        testingPostgresqlServer = new TestingEnvironmentPg10();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig(), "set time zone 'UTC'");

        EventBus eventBus = new EventBus();
        metastore = new PostgresqlMetastore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), eventBus, new ProjectConfig());

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(new ProjectConfig(), dataSource, metastore, new CustomDataSourceService(dataSource), false);

        PostgresqlMaterializedViewService materializedViewService = new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore, Clock.systemUTC());

        QueryExecutorService queryExecutorService = new QueryExecutorService(queryExecutor, metastore, materializedViewService, '"');
        configManager = new PostgresqlConfigManager(dataSource);
        configManager.setup();
        PostgresqlUserStorage userStorage = new PostgresqlUserStorage(queryExecutorService, materializedViewService, configManager, queryExecutor);
        PostgresqlEventStore postgresqlEventStore = new PostgresqlEventStore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), new FieldDependencyBuilder().build());
        userService = new PostgresqlUserService(new ProjectConfig(), configManager, postgresqlEventStore, userStorage, metastore, queryExecutor);
        super.setUp();
    }

    @Override
    public AbstractUserService getUserService() {
        return userService;
    }

    @Override
    public ConfigManager getConfigManager() {
        return configManager;
    }

    @Override
    public Metastore getMetastore() {
        return metastore;
    }
}
