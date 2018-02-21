package org.rakam;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.TestMaterializedView;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoMaterializedViewService;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.presto.analysis.PrestoRakamRaptorMetastore;
import org.rakam.report.QueryExecutor;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class TestPrestoMaterializedView extends TestMaterializedView {
    private TestingEnvironment testingEnvironment;
    private PrestoRakamRaptorMetastore metastore;
    private PrestoQueryExecutor prestoQueryExecutor;
    private InMemoryQueryMetadataStore queryMetadataStore;
    private TestingPrestoEventStore eventStore;
    private PrestoMaterializedViewService materializedViewService;

    @BeforeSuite
    @Override
    public void setup() throws Exception {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();

        queryMetadataStore = new InMemoryQueryMetadataStore();

        metastore = new PrestoRakamRaptorMetastore(testingEnvironment.getPrestoMetastore(), new EventBus(), new ProjectConfig(), prestoConfig);
        metastore.setup();

        prestoQueryExecutor = new PrestoQueryExecutor(new ProjectConfig(), prestoConfig, null, metastore);

        materializedViewService = new PrestoMaterializedViewService(
                new PrestoConfig(),
                prestoQueryExecutor, metastore, queryMetadataStore, Clock.systemUTC());

        eventStore = new TestingPrestoEventStore(prestoQueryExecutor, prestoConfig);

        super.setup();
    }

    // TODO: the cache type in presto doesn't work properly because of lack of support of atomic REFRESH MATERIALIZED VIEW support. It sometimes returns empty result during the refresh phase.
    @Test
    public void testCacheRenewConcurrentInSingle()
            throws Exception {
        //
    }

    @Override
    public Metastore getMetastore() {
        return metastore;
    }

    @Override
    public IncrementableClock getClock() {
        return new JVMClock();
    }

    @Override
    public MaterializedViewService getMaterializedViewService() {
        return materializedViewService;
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        return prestoQueryExecutor;
    }

    @Override
    public EventStore getEventStore() {
        return eventStore;
    }

    public static class JVMClock extends IncrementableClock {

        @Override
        public void increment(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant() {
            return Instant.now();
        }
    }
}
