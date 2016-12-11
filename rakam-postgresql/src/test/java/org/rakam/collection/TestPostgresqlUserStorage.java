package org.rakam.collection;

import com.google.common.eventbus.EventBus;
import org.rakam.TestingEnvironment;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InMemoryEventStore;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.postgresql.PostgresqlConfigManager;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.plugin.user.PostgresqlUserService;
import org.rakam.postgresql.plugin.user.PostgresqlUserStorage;
import org.rakam.postgresql.report.PostgresqlPseudoContinuousQueryService;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPostgresqlUserStorage
        extends TestUserStorage
{
    private TestingEnvironment testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlUserService userService;
    private PostgresqlConfigManager configManager;

    @BeforeSuite
    @Override
    public void setUp()
            throws Exception
    {
        testingPostgresqlServer = new TestingEnvironment();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig(), "set time zone 'UTC'");

        EventBus eventBus = new EventBus();
        metastore = new PostgresqlMetastore(dataSource, eventBus);

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, metastore, false);

        PostgresqlMaterializedViewService materializedViewService = new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore);

        configManager = new PostgresqlConfigManager(dataSource);
        configManager.setup();
        PostgresqlUserStorage userStorage = new PostgresqlUserStorage(materializedViewService, configManager, queryExecutor);
        userService = new PostgresqlUserService(userStorage, metastore, queryExecutor);
        super.setUp();
    }

    @Override
    public AbstractUserService getUserService()
    {
        return userService;
    }

    @Override
    public ConfigManager getConfigManager()
    {
        return configManager;
    }

    @Override
    public Metastore getMetastore()
    {
        return metastore;
    }
}
