package org.rakam.pg10.collection;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.TestUserStorage;
import org.rakam.config.ProjectConfig;
import org.rakam.pg10.TestingEnvironmentPg10;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.postgresql.PostgresqlConfigManager;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.plugin.user.PostgresqlUserService;
import org.rakam.postgresql.plugin.user.PostgresqlUserStorage;
import org.testng.annotations.BeforeSuite;

public class TestPostgresqlUserStorage
        extends TestUserStorage {
    private TestingEnvironmentPg10 testingPostgresqlServer;
    private PostgresqlUserService userService;
    private PostgresqlConfigManager configManager;
    private PostgresqlMetastore metastore;

    @BeforeSuite
    @Override
    public void setUp()
            throws Exception {
        testingPostgresqlServer = new TestingEnvironmentPg10();

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
