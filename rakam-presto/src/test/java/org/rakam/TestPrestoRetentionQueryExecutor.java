package org.rakam;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.TestRetentionQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.JDBCConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoContinuousQueryService;
import org.rakam.presto.analysis.PrestoMaterializedViewService;
import org.rakam.presto.analysis.PrestoMetastore;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.presto.analysis.PrestoRetentionQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPrestoRetentionQueryExecutor extends TestRetentionQueryExecutor {

    private TestingEnvironment testingEnvironment;
    private PrestoMetastore metastore;
    private PrestoRetentionQueryExecutor retentionQueryExecutor;
    private TestingPrestoEventStore testingPrestoEventStore;

    @BeforeSuite
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();
        JDBCConfig postgresqlConfig = testingEnvironment.getPostgresqlConfig();
        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();

        JDBCPoolDataSource metastoreDataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);
        metastore = new PrestoMetastore(testingEnvironment.getPrestoMetastore(),
                metastoreDataSource,
                new EventBus(), new FieldDependencyBuilder().build(), prestoConfig);
        metastore.setup();

        PrestoQueryExecutor queryExecutor = new PrestoQueryExecutor(prestoConfig, metastore);
        PrestoMaterializedViewService materializedViewService = new PrestoMaterializedViewService(testingEnvironment.getPrestoMetastore(),
                queryExecutor, metastore, queryMetadataStore);
        PrestoContinuousQueryService continuousQueryService = new PrestoContinuousQueryService(queryMetadataStore, queryExecutor, prestoConfig);

        QueryExecutorService queryExecutorService = new QueryExecutorService(queryExecutor, queryMetadataStore, metastore, materializedViewService, Clock.systemUTC());

        retentionQueryExecutor = new PrestoRetentionQueryExecutor(queryExecutorService, metastore, materializedViewService, continuousQueryService);
        testingPrestoEventStore = new TestingPrestoEventStore(queryExecutor, prestoConfig);

        // TODO: Presto throws "No node available" error, find a way to avoid this ugly hack.
        Thread.sleep(1000);
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
    public RetentionQueryExecutor getRetentionQueryExecutor() {
        return retentionQueryExecutor;
    }
}
