package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.TestingEnvironment;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.report.PostgresqlPseudoContinuousQueryService;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPostgresqlContinuousQueryService extends TestContinuousQueryService {

    private TestingEnvironment testingPostgresqlServer;
    private PostgresqlPseudoContinuousQueryService continuousQueryService;
    private PostgresqlMetastore metastore;

    @BeforeSuite
    public void setUp() throws Exception {
        testingPostgresqlServer = new TestingEnvironment();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig());

        metastore = new PostgresqlMetastore(dataSource, new EventBus());

        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, metastore, new CustomDataSourceService(dataSource), false);
        QueryExecutorService executorService = new QueryExecutorService(queryExecutor, metastore,
                new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore), Clock.systemUTC(), '"');
        continuousQueryService = new PostgresqlPseudoContinuousQueryService(queryMetadataStore, executorService, queryExecutor);
    }

    @Override
    public ContinuousQueryService getContinuousQueryService() {
        return continuousQueryService;
    }

    @Override
    public Metastore getMetastore() {
        return metastore;
    }
}
