package org.rakam.analysis;

import com.google.common.eventbus.EventBus;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.report.eventexplorer.EventExplorerListener;
import org.rakam.postgresql.analysis.PostgresqlEventStore;
import org.rakam.postgresql.analysis.PostgresqlMaterializedViewService;
import org.rakam.postgresql.analysis.PostgresqlMetastore;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.JDBCConfig;
import org.rakam.report.QueryExecutorService;
import org.rakam.postgresql.report.PostgresqlEventExplorer;
import org.rakam.postgresql.report.PostgresqlPseudoContinuousQueryService;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.time.Clock;

public class TestPostgresqlEventExplorer extends TestEventExplorer {

    private TestingPostgreSqlServer testingPostgresqlServer;
    private JDBCConfig postgresqlConfig;
    private PostgresqlMetastore metastore;
    private PostgresqlEventStore eventStore;
    private PostgresqlEventExplorer eventExplorer;

    @Override
    @BeforeSuite
    public void setup() throws Exception {
        testingPostgresqlServer = new TestingPostgreSqlServer("testuser", "testdb");
        postgresqlConfig = new JDBCConfig()
                .setUrl(testingPostgresqlServer.getJdbcUrl())
                .setUsername(testingPostgresqlServer.getUser());

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);
        PostgresqlQueryExecutor queryExecutor = new PostgresqlQueryExecutor(dataSource, queryMetadataStore);

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        EventBus eventBus = new EventBus();

        QueryExecutorService executorService = new QueryExecutorService(queryExecutor, queryMetadataStore, metastore,
                new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore, Clock.systemUTC()));
        PostgresqlPseudoContinuousQueryService continuousQueryService = new PostgresqlPseudoContinuousQueryService(queryMetadataStore, executorService, queryExecutor);
        eventBus.register(new EventExplorerListener(continuousQueryService));

        metastore = new PostgresqlMetastore(dataSource, eventBus, build);

        eventStore = new PostgresqlEventStore(dataSource, build);
        PostgresqlMaterializedViewService materializedViewService = new PostgresqlMaterializedViewService(queryExecutor, queryMetadataStore, Clock.systemUTC());
        eventExplorer = new PostgresqlEventExplorer(
                new QueryExecutorService(queryExecutor, queryMetadataStore, metastore, materializedViewService),
                queryExecutor, metastore);

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
    public EventExplorer getEventExplorer() {
        return eventExplorer;
    }
}
