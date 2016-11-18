package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.TestingEnvironment;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;
import org.rakam.postgresql.PostgresqlConfigManager;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.analysis.PostgresqlRetentionQueryExecutor;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPostgresqlRetentionQueryExecutor extends TestRetentionQueryExecutor {

    private TestingEnvironment testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlRetentionQueryExecutor retentionQueryExecutor;

    @BeforeSuite
    public void setup() throws Exception {
        testingPostgresqlServer = new TestingEnvironment();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig());

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        metastore = new PostgresqlMetastore(dataSource, new EventBus(), build);

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, metastore, false, queryMetadataStore);
        eventStore = new PostgresqlEventStore(dataSource, build);

        PostgresqlConfigManager testConfigManager = new PostgresqlConfigManager(dataSource);

        retentionQueryExecutor = new PostgresqlRetentionQueryExecutor(testConfigManager, queryExecutor, metastore);
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
