package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlFunnelQueryExecutor;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.JDBCConfig;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;

public class TestPostgresqlFunnelQueryExecutor extends TestFunnelQueryExecutor {

    private TestingPostgreSqlServer testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlFunnelQueryExecutor funnelQueryExecutor;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingPostgresqlServer = new TestingPostgreSqlServer("testuser", "testdb");
        JDBCConfig postgresqlConfig = new JDBCConfig()
                .setUrl(testingPostgresqlServer.getJdbcUrl())
                .setUsername(testingPostgresqlServer.getUser());

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        metastore = new PostgresqlMetastore(dataSource, new EventBus(), build);

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, queryMetadataStore);
        eventStore = new PostgresqlEventStore(dataSource, build);
        funnelQueryExecutor = new PostgresqlFunnelQueryExecutor(queryExecutor);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                testingPostgresqlServer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

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
