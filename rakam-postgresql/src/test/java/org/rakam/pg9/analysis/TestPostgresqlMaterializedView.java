package org.rakam.pg9.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.PGClock;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.TestMaterializedView;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.ProjectConfig;
import org.rakam.pg9.TestingEnvironmentPg9;
import org.rakam.plugin.EventStore;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutor;
import org.testng.annotations.BeforeSuite;

public class TestPostgresqlMaterializedView extends TestMaterializedView {
    private TestingEnvironmentPg9 testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlQueryExecutor queryExecutor;
    private InMemoryQueryMetadataStore queryMetadataStore;

    @BeforeSuite
    public void setup() throws Exception {
        testingPostgresqlServer = new TestingEnvironmentPg9();

        queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig(), "set time zone 'UTC'");

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        EventBus eventBus = new EventBus();

        metastore = new PostgresqlMetastore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), eventBus, new ProjectConfig());
        queryExecutor = new PostgresqlQueryExecutor(new ProjectConfig(), dataSource, metastore, new CustomDataSourceService(dataSource), false);

        eventStore = new PostgresqlEventStore(dataSource, new PostgresqlModule.PostgresqlVersion(dataSource), build);
        super.setup();
    }

    @Override
    public Metastore getMetastore() {
        return metastore;
    }

    @Override
    public IncrementableClock getClock() {
        return new PGClock(queryExecutor);
    }

    @Override
    public MaterializedViewService getMaterializedViewService() {
        return new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore, new PGClock(queryExecutor));
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    @Override
    public EventStore getEventStore() {
        return eventStore;
    }

}
