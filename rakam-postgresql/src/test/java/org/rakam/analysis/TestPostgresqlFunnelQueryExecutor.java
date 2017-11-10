package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.TestingEnvironment;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.postgresql.analysis.FastGenericFunnelQueryExecutor;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlFunnelQueryExecutor;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPostgresqlFunnelQueryExecutor extends TestFunnelQueryExecutor {

    private TestingEnvironment testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlFunnelQueryExecutor funnelQueryExecutor;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingPostgresqlServer = new TestingEnvironment();

        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig());

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        metastore = new PostgresqlMetastore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), new EventBus());

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(new ProjectConfig(), dataSource, metastore, new CustomDataSourceService(dataSource), false);
        eventStore = new PostgresqlEventStore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), build);
        FastGenericFunnelQueryExecutor exec = new FastGenericFunnelQueryExecutor(new QueryExecutorService(queryExecutor, metastore, null, Clock.systemUTC(), '"'),
                new ProjectConfig(), metastore);

        funnelQueryExecutor = new PostgresqlFunnelQueryExecutor(exec, new ProjectConfig(), metastore, queryExecutor);
        funnelQueryExecutor.setup();
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
    public FunnelQueryExecutor getFunnelQueryExecutor() {
        return funnelQueryExecutor;
    }
}
