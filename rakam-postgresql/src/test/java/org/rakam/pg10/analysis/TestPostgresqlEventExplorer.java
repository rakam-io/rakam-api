package org.rakam.pg10.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestEventExplorer;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.ProjectConfig;
import org.rakam.pg10.TestingEnvironmentPg10;
import org.rakam.plugin.EventStore;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.report.PostgresqlEventExplorer;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPostgresqlEventExplorer
        extends TestEventExplorer {

    private TestingEnvironmentPg10 testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlEventExplorer eventExplorer;

    @Override
    @BeforeSuite
    public void setup()
            throws Exception {
        testingPostgresqlServer = new TestingEnvironmentPg10();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig(), "set time zone 'UTC'");

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        EventBus eventBus = new EventBus();

        metastore = new PostgresqlMetastore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), eventBus, new ProjectConfig());
        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(new ProjectConfig(), dataSource, metastore, new CustomDataSourceService(dataSource), false);

        PostgresqlMaterializedViewService postgresqlMaterializedViewService = new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore, Clock.systemUTC());

        eventStore = new PostgresqlEventStore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), build);
        PostgresqlMaterializedViewService materializedViewService = postgresqlMaterializedViewService;
        eventExplorer = new PostgresqlEventExplorer(
                new ProjectConfig(),
                new QueryExecutorService(queryExecutor, metastore, materializedViewService, Clock.systemUTC(), '"'),
                materializedViewService);
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
    public EventExplorer getEventExplorer() {
        return eventExplorer;
    }
}
