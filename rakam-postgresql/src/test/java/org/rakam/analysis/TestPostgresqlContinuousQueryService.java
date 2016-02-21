package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.JDBCConfig;
import org.rakam.report.QueryExecutorService;
import org.rakam.postgresql.report.PostgresqlPseudoContinuousQueryService;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.time.Clock;

public class TestPostgresqlContinuousQueryService extends TestContinuousQueryService {

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
        QueryExecutorService executorService = new QueryExecutorService(queryExecutor, queryMetadataStore, metastore,
                new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore, Clock.systemUTC()));
        continuousQueryService = new PostgresqlPseudoContinuousQueryService(queryMetadataStore, executorService, queryExecutor);

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
