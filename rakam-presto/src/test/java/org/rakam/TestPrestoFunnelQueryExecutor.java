package org.rakam;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestFunnelQueryExecutor;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.postgresql.analysis.FastGenericFunnelQueryExecutor;
import org.rakam.presto.analysis.*;
import org.rakam.report.QueryExecutorService;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;

public class TestPrestoFunnelQueryExecutor extends TestFunnelQueryExecutor {

    private FunnelQueryExecutor funnelQueryExecutor;
    private TestingPrestoEventStore testingPrestoEventStore;
    private TestingEnvironment testingEnvironment;
    private PrestoRakamRaptorMetastore metastore;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();

        InMemoryQueryMetadataStore inMemoryQueryMetadataStore = new InMemoryQueryMetadataStore();
        JDBCPoolDataSource prestoMetastore = testingEnvironment.getPrestoMetastore();

        EventBus eventBus = new EventBus();

        metastore = new PrestoRakamRaptorMetastore(prestoMetastore, eventBus, new ProjectConfig(), prestoConfig);
        metastore.setup();

        PrestoQueryExecutor prestoQueryExecutor = new PrestoQueryExecutor(new ProjectConfig(), prestoConfig, null, metastore);

        PrestoMaterializedViewService materializedViewService = new PrestoMaterializedViewService(
                new PrestoConfig(),
                prestoQueryExecutor, metastore, inMemoryQueryMetadataStore, Clock.systemUTC());
        QueryExecutorService queryExecutorService = new QueryExecutorService(prestoQueryExecutor, metastore,
                materializedViewService, '"');

        FastGenericFunnelQueryExecutor fastGenericFunnelQueryExecutor = new FastGenericFunnelQueryExecutor(queryExecutorService, new ProjectConfig(), metastore);
        PrestoApproxFunnelQueryExecutor prestoApproxFunnelQueryExecutor = new PrestoApproxFunnelQueryExecutor(new ProjectConfig(), queryExecutorService, metastore);
        funnelQueryExecutor = new PrestoFunnelQueryExecutor(new ProjectConfig(), new PrestoConfig(),
                fastGenericFunnelQueryExecutor, prestoApproxFunnelQueryExecutor, metastore, prestoQueryExecutor,
                new UserPluginConfig());
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
    public FunnelQueryExecutor getFunnelQueryExecutor() {
        return funnelQueryExecutor;
    }
}
