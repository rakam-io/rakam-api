package org.rakam;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestEventExplorer;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.JDBCConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoContinuousQueryService;
import org.rakam.presto.analysis.PrestoEventExplorer;
import org.rakam.presto.analysis.PrestoMaterializedViewService;
import org.rakam.presto.analysis.PrestoMetastore;
import org.rakam.presto.analysis.PrestoQueryExecution;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.eventexplorer.EventExplorerListener;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;
import java.util.Map;

public class TestPrestoEventExplorer extends TestEventExplorer {

    private EventExplorer eventExplorer;
    private TestingPrestoEventStore testingPrestoEventStore;
    private TestingEnvironment testingEnvironment;
    private PrestoMetastore metastore;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();
        JDBCConfig postgresqlConfig = testingEnvironment.getPostgresqlConfig();

        JDBCPoolDataSource metastoreDataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);
        InMemoryQueryMetadataStore inMemoryQueryMetadataStore = new InMemoryQueryMetadataStore();

        EventBus eventBus = new EventBus();

        metastore = new PrestoMetastore(testingEnvironment.getPrestoMetastore(),
                metastoreDataSource,
                eventBus, new FieldDependencyBuilder().build(), prestoConfig);
        metastore.setup();

        PrestoQueryExecutor prestoQueryExecutor = new PrestoQueryExecutor(prestoConfig, metastore) {
            @Override
            public PrestoQueryExecution executeRawQuery(String query, Map<String, String> sessionProperties) {
                // ignore session parameter in tests
                return super.executeRawQuery(query);
            }
        };

        PrestoContinuousQueryService continuousQueryService = new PrestoContinuousQueryService(inMemoryQueryMetadataStore,
                prestoQueryExecutor, prestoConfig);
        eventBus.register(new EventExplorerListener(continuousQueryService));

        PrestoMaterializedViewService materializedViewService = new PrestoMaterializedViewService(testingEnvironment.getPrestoMetastore(),
                prestoQueryExecutor, metastore, inMemoryQueryMetadataStore);
        QueryExecutorService queryExecutorService = new QueryExecutorService(prestoQueryExecutor,
                inMemoryQueryMetadataStore, metastore, materializedViewService,  Clock.systemUTC());

        eventExplorer = new PrestoEventExplorer(queryExecutorService, continuousQueryService,
                materializedViewService, metastore);
        testingPrestoEventStore = new TestingPrestoEventStore(prestoQueryExecutor, prestoConfig);

        super.setup();
    }

    @Override
    public EventStore getEventStore() {
        return testingPrestoEventStore;
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
