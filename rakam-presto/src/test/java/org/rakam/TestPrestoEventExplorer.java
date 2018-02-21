package org.rakam;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestEventExplorer;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.JDBCConfig;
import org.rakam.config.ProjectConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.*;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPrestoEventExplorer
        extends TestEventExplorer {
    private EventExplorer eventExplorer;
    private TestingEnvironment testingEnvironment;
    private PrestoRakamRaptorMetastore metastore;
    private PrestoQueryExecutor prestoQueryExecutor;
    private InMemoryQueryMetadataStore queryMetadataStore;
    private JDBCPoolDataSource metastoreDataSource;
    private TestingPrestoEventStore eventStore;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();
        JDBCConfig postgresqlConfig = testingEnvironment.getPostgresqlConfig();

        metastoreDataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);
        queryMetadataStore = new InMemoryQueryMetadataStore();

        metastore = new PrestoRakamRaptorMetastore(testingEnvironment.getPrestoMetastore(), new EventBus(), new ProjectConfig(), prestoConfig);
        metastore.setup();

        prestoQueryExecutor = new PrestoQueryExecutor(new ProjectConfig(), prestoConfig, null, metastore);

        PrestoMaterializedViewService materializedViewService = new PrestoMaterializedViewService(
                new PrestoConfig(),
                prestoQueryExecutor, metastore, queryMetadataStore, Clock.systemUTC());
        QueryExecutorService queryExecutorService = new QueryExecutorService(prestoQueryExecutor, metastore, materializedViewService, '"');

        eventExplorer = new PrestoEventExplorer(new ProjectConfig(), queryExecutorService, materializedViewService);
        eventStore = new TestingPrestoEventStore(prestoQueryExecutor, prestoConfig);

        super.setup();
    }

    @Override
    public EventStore getEventStore() {
        return eventStore;
    }

    public PrestoQueryExecutor getPrestoQueryExecutor() {
        return prestoQueryExecutor;
    }

    public JDBCPoolDataSource getMetastoreDataSource() {
        return metastoreDataSource;
    }

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
