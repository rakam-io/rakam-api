package org.rakam.pg9.collection;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.TestUserStorage;
import org.rakam.pg9.TestingEnvironmentPg9;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.postgresql.PostgresqlConfigManager;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.plugin.user.PostgresqlUserService;
import org.rakam.postgresql.plugin.user.PostgresqlUserStorage;
import org.testng.annotations.BeforeSuite;

public class TestPostgresqlUserStorage
        extends TestUserStorage {
    private TestingEnvironmentPg9 testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlUserService userService;
    private PostgresqlConfigManager configManager;

    @BeforeSuite
    @Override
    public void setUp()
            throws Exception {
        testingPostgresqlServer = new TestingEnvironmentPg9();

        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig(), "set time zone 'UTC'");

        EventBus eventBus = new EventBus();
        metastore = new PostgresqlMetastore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), eventBus);

        configManager = new PostgresqlConfigManager(dataSource);
        configManager.setup();

        PostgresqlUserStorage userStorage = new PostgresqlUserStorage(dataSource, configManager);
        userService = new PostgresqlUserService(userStorage);
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
