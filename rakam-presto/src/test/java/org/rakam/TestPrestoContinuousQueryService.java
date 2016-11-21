package org.rakam;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.TestContinuousQueryService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.event.TestingEnvironment;
import org.rakam.presto.analysis.PrestoContinuousQueryService;
import org.rakam.presto.analysis.PrestoMetastore;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.report.realtime.RealTimeConfig;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(enabled=false)
public class TestPrestoContinuousQueryService  {
    private PrestoContinuousQueryService continuousQueryService;
    private Metastore metastore;
    private TestingEnvironment testEnvironment;

//    @BeforeSuite
    public void setUp() throws Exception {
        testEnvironment = new TestingEnvironment();

        metastore = new PrestoMetastore(testEnvironment.getPrestoMetastore(),
                new EventBus(), new FieldDependencyBuilder().build(), testEnvironment.getPrestoConfig());
        metastore.setup();

        InMemoryQueryMetadataStore queryMetadataStore = new InMemoryQueryMetadataStore();

        PrestoQueryExecutor prestoQueryExecutor = new PrestoQueryExecutor(testEnvironment.getPrestoConfig(), null, null, metastore);

        continuousQueryService = new PrestoContinuousQueryService(queryMetadataStore, new RealTimeConfig(),
                prestoQueryExecutor, testEnvironment.getPrestoConfig());
    }

//    @Override
    public ContinuousQueryService getContinuousQueryService() {
        return continuousQueryService;
    }

//    @Override
    public Metastore getMetastore() {
        return metastore;
    }
}
