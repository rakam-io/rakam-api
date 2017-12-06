package org.rakam.pg10.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.TestRetentionQueryExecutor;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.ProjectConfig;
import org.rakam.pg10.TestingEnvironmentPg10;
import org.rakam.plugin.EventStore;
import org.rakam.postgresql.PostgresqlModule.PostgresqlVersion;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.analysis.PostgresqlRetentionQueryExecutor;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.testng.annotations.BeforeSuite;

public class TestPostgresqlRetentionQueryExecutor extends TestRetentionQueryExecutor {

    private TestingEnvironmentPg10 testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlRetentionQueryExecutor retentionQueryExecutor;

    @BeforeSuite
    public void setup() throws Exception {
        testingPostgresqlServer = new TestingEnvironmentPg10();

        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig());

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        metastore = new PostgresqlMetastore(dataSource, new PostgresqlVersion(dataSource), new EventBus(), new ProjectConfig());

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(new ProjectConfig(), dataSource, metastore, new CustomDataSourceService(dataSource), false);
        eventStore = new PostgresqlEventStore(dataSource, new PostgresqlVersion(dataSource), build);

        retentionQueryExecutor = new PostgresqlRetentionQueryExecutor(new ProjectConfig(), queryExecutor, metastore);
        retentionQueryExecutor.setup();

        super.setup();
    }

    @Override
    public EventStore getEventStore() {
        return eventStore;
    }

    @Override
    public Metastore getMetastore() {
        return metastore;
    }

    @Override
    public RetentionQueryExecutor getRetentionQueryExecutor() {
        return retentionQueryExecutor;
    }
}
