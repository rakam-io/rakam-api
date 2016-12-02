package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.TestingEnvironment;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.postgresql.report.PostgresqlEventExplorer;
import org.rakam.postgresql.report.PostgresqlPseudoContinuousQueryService;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPostgresqlEventExplorer
        extends TestEventExplorer
{

    private TestingEnvironment testingPostgresqlServer;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlEventExplorer eventExplorer;

    @Override
    @BeforeSuite
    public void setup()
            throws Exception
    {
        testingPostgresqlServer = new TestingEnvironment();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(testingPostgresqlServer.getPostgresqlConfig(), "set time zone 'UTC'");

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        EventBus eventBus = new EventBus();

        metastore = new PostgresqlMetastore(dataSource, eventBus);
        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, metastore, false, queryMetadataStore);

        QueryExecutorService executorService = new QueryExecutorService(queryExecutor, metastore,
                new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore), Clock.systemUTC(), '"');
        PostgresqlPseudoContinuousQueryService continuousQueryService = new PostgresqlPseudoContinuousQueryService(queryMetadataStore, executorService, queryExecutor);

        eventStore = new PostgresqlEventStore(dataSource, build);
        PostgresqlMaterializedViewService materializedViewService = new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore);
        eventExplorer = new PostgresqlEventExplorer(
                new QueryExecutorService(queryExecutor, metastore, materializedViewService, Clock.systemUTC(), '"'),
                materializedViewService,
                continuousQueryService);
        super.setup();
    }

    @Override
    public EventStore getEventStore()
    {
        return eventStore;
    }

    @Override
    public Metastore getMetastore()
    {
        return metastore;
    }

    @Override
    public EventExplorer getEventExplorer()
    {
        return eventExplorer;
    }
}
