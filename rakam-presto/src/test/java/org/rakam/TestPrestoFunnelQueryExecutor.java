package org.rakam;

import com.facebook.presto.hadoop.shaded.com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestFunnelQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.JDBCConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.JDBCMetastore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoContinuousQueryService;
import org.rakam.presto.analysis.PrestoFunnelQueryExecutor;
import org.rakam.presto.analysis.PrestoMaterializedViewService;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.eventexplorer.EventExplorerListener;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPrestoFunnelQueryExecutor extends TestFunnelQueryExecutor {

    private FunnelQueryExecutor funnelQueryExecutor;
    private TestingPrestoEventStore testingPrestoEventStore;
    private TestingEnvironment testingEnvironment;
    private JDBCMetastore metastore;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();
        JDBCConfig postgresqlConfig = testingEnvironment.getPostgresqlConfig();

        JDBCPoolDataSource metastoreDataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);
        InMemoryQueryMetadataStore inMemoryQueryMetadataStore = new InMemoryQueryMetadataStore();

        EventBus eventBus = new EventBus();

        metastore = new JDBCMetastore(metastoreDataSource, prestoConfig,
                eventBus, new FieldDependencyBuilder().build());
        metastore.setup();

        PrestoQueryExecutor prestoQueryExecutor = new PrestoQueryExecutor(prestoConfig, metastore);

        PrestoContinuousQueryService continuousQueryService = new PrestoContinuousQueryService(inMemoryQueryMetadataStore,
                prestoQueryExecutor, prestoConfig);
        eventBus.register(new EventExplorerListener(continuousQueryService));

        QueryExecutorService queryExecutorService = new QueryExecutorService(prestoQueryExecutor, inMemoryQueryMetadataStore, metastore,
                new PrestoMaterializedViewService(prestoQueryExecutor, inMemoryQueryMetadataStore, Clock.systemUTC()));

        funnelQueryExecutor = new PrestoFunnelQueryExecutor(queryExecutorService);
        testingPrestoEventStore = new TestingPrestoEventStore(prestoQueryExecutor, prestoConfig);
        Thread.sleep(1000);
        super.setup();
    }

    @AfterSuite
    public void destroy() {
        super.destroy();
        try {
            testingEnvironment.close();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
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
    public FunnelQueryExecutor getFunnelQueryExecutor() {
        return funnelQueryExecutor;
    }
}
