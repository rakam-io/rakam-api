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
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoContinuousQueryService;
import org.rakam.presto.analysis.PrestoEventExplorer;
import org.rakam.presto.analysis.PrestoMaterializedViewService;
import org.rakam.presto.analysis.PrestoMetastore;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.presto.plugin.EventExplorerListener;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.realtime.RealTimeConfig;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public abstract class TestPrestoEventExplorer extends TestEventExplorer {

    private EventExplorer eventExplorer;
    private TestingEnvironment testingEnvironment;
    private PrestoMetastore metastore;
    private PrestoQueryExecutor prestoQueryExecutor;
    private InMemoryQueryMetadataStore queryMetadataStore;
    private JDBCPoolDataSource metastoreDataSource;
    private PrestoContinuousQueryService continuousQueryService;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();
        JDBCConfig postgresqlConfig = testingEnvironment.getPostgresqlConfig();

        metastoreDataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);
        queryMetadataStore = new InMemoryQueryMetadataStore();

        FieldDependencyBuilder.FieldDependency build = new FieldDependencyBuilder().build();
        metastore = new PrestoMetastore(testingEnvironment.getPrestoMetastore(),
                new EventBus(), build, prestoConfig);
        metastore.setup();

        prestoQueryExecutor = new PrestoQueryExecutor(prestoConfig, metastore);

        continuousQueryService = new PrestoContinuousQueryService(queryMetadataStore, new RealTimeConfig(),
                prestoQueryExecutor, prestoConfig);

        PrestoMaterializedViewService materializedViewService = new PrestoMaterializedViewService(testingEnvironment.getPrestoMetastore(),
                prestoQueryExecutor, metastore, queryMetadataStore);
        QueryExecutorService queryExecutorService = new QueryExecutorService(prestoQueryExecutor, metastore, materializedViewService,  Clock.systemUTC(), '"');

        eventExplorer = new PrestoEventExplorer(queryExecutorService, continuousQueryService, materializedViewService);
        setupInline();
        super.setup();
        new EventExplorerListener(continuousQueryService).onCreateProject(new ProjectCreatedEvent(PROJECT_NAME));
        // todo find a better way of handling this
        Thread.sleep(20000);
    }

    public PrestoQueryExecutor getPrestoQueryExecutor() {
        return prestoQueryExecutor;
    }

    public PrestoContinuousQueryService getContinuousQueryService() {
        return continuousQueryService;
    }

    public JDBCPoolDataSource getMetastoreDataSource() {
        return metastoreDataSource;
    }

    public abstract void setupInline();

    @Override
    public Metastore getMetastore() {
        return metastore;
    }

    @Override
    public EventExplorer getEventExplorer() {
        return eventExplorer;
    }

    public TestingEnvironment getEnvironment() {
        return testingEnvironment;
    }
}
