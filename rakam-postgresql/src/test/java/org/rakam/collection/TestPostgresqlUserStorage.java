package org.rakam.collection;

import com.google.common.eventbus.EventBus;
import org.rakam.TestingEnvironment;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.user.AbstractUserService;
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

    @BeforeSuite
    @Override
    public void setUp()
            throws Exception
    {
        testingPostgresqlServer = new TestingEnvironment();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig(), "set time zone 'UTC'");

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        EventBus eventBus = new EventBus();
        metastore = new PostgresqlMetastore(dataSource, eventBus, build);

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, metastore, queryMetadataStore);

        PostgresqlMaterializedViewService materializedViewService = new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore);

        PostgresqlConfigManager configManager = new PostgresqlConfigManager(dataSource);
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
    public Metastore getMetastore()
    {
        return metastore;
    }
}
