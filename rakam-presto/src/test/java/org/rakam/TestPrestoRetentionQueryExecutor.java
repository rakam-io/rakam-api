package org.rakam;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.TestRetentionQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.presto.analysis.*;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPrestoRetentionQueryExecutor extends TestRetentionQueryExecutor {

    private TestingEnvironment testingEnvironment;
    private PrestoRakamRaptorMetastore metastore;
    private PrestoRetentionQueryExecutor retentionQueryExecutor;
    private TestingPrestoEventStore testingPrestoEventStore;

    @BeforeSuite
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();
        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();

        metastore = new PrestoRakamRaptorMetastore(testingEnvironment.getPrestoMetastore(), new EventBus(), new ProjectConfig(), prestoConfig);
        metastore.setup();

        PrestoQueryExecutor queryExecutor = new PrestoQueryExecutor(new ProjectConfig(), prestoConfig, null, metastore);
        PrestoMaterializedViewService materializedViewService = new PrestoMaterializedViewService(
                new PrestoConfig(),
                queryExecutor, metastore, queryMetadataStore, Clock.systemUTC());

        QueryExecutorService queryExecutorService = new QueryExecutorService(queryExecutor, metastore, materializedViewService, '"');

        retentionQueryExecutor = new PrestoRetentionQueryExecutor(new ProjectConfig(), queryExecutorService, metastore, materializedViewService, new UserPluginConfig());
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
