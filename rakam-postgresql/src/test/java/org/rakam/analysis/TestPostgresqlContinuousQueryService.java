package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.analysis.postgresql.PostgresqlMetastore;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.JDBCConfig;
import org.rakam.report.postgresql.PostgresqlPseudoContinuousQueryService;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;

public class TestPostgresqlContinuousQueryService  extends TestContinuousQueryService {

    private TestingPostgreSqlServer testingPostgresqlServer;
    private PostgresqlPseudoContinuousQueryService continuousQueryService;
    private PostgresqlMetastore metastore;

    @BeforeSuite
    public void setUp() throws Exception {
        testingPostgresqlServer = new TestingPostgreSqlServer("testuser", "testdb");
        JDBCConfig postgresqlConfig = new JDBCConfig()
                .setUrl(testingPostgresqlServer.getJdbcUrl())
                .setUsername(testingPostgresqlServer.getUser());

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);

        metastore = new PostgresqlMetastore(dataSource, new EventBus(), new FieldDependencyBuilder().build());

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, queryMetadataStore);
        continuousQueryService = new PostgresqlPseudoContinuousQueryService(queryMetadataStore, queryExecutor);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                testingPostgresqlServer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    @Override
    public ContinuousQueryService getContinuousQueryService() {
        return continuousQueryService;
    }

    @Override
    public Metastore getMetastore() {
        return metastore;
    }

    @AfterSuite
    public void tearDown() throws Exception {
        testingPostgresqlServer.close();
    }
}
