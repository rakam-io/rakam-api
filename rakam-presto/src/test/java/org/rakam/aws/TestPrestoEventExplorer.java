package org.rakam.aws;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.InMemoryQueryMetadataStore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestEventExplorer;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.kinesis.AWSKinesisEventStore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.JDBCConfig;
import org.rakam.config.ProjectConfig;
import org.rakam.event.TestingEnvironment;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoContinuousQueryService;
import org.rakam.presto.analysis.PrestoEventExplorer;
import org.rakam.presto.analysis.PrestoMaterializedViewService;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.presto.analysis.PrestoRakamRaptorMetastore;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.realtime.RealTimeConfig;
import org.testng.annotations.BeforeSuite;

import java.time.Clock;
import java.util.List;

public class TestPrestoEventExplorer
        extends TestEventExplorer
{
    private EventExplorer eventExplorer;
    private TestingEnvironment testingEnvironment;
    private PrestoRakamRaptorMetastore metastore;
    private PrestoQueryExecutor prestoQueryExecutor;
    private InMemoryQueryMetadataStore queryMetadataStore;
    private JDBCPoolDataSource metastoreDataSource;
    private PrestoContinuousQueryService continuousQueryService;
    private AWSKinesisEventStore testingPrestoEventStore;

    @BeforeSuite
    @Override
    public void setup()
            throws Exception
    {
        testingEnvironment = new TestingEnvironment();
        PrestoConfig prestoConfig = testingEnvironment.getPrestoConfig();
        JDBCConfig postgresqlConfig = testingEnvironment.getPostgresqlConfig();

        metastoreDataSource = JDBCPoolDataSource.getOrCreateDataSource(postgresqlConfig);
        queryMetadataStore = new InMemoryQueryMetadataStore();

        metastore = new PrestoRakamRaptorMetastore(testingEnvironment.getPrestoMetastore(), new EventBus(), new ProjectConfig(), prestoConfig);
        metastore.setup();

        prestoQueryExecutor = new PrestoQueryExecutor(new ProjectConfig(), prestoConfig, null, null, metastore);

        continuousQueryService = new PrestoContinuousQueryService(queryMetadataStore, new RealTimeConfig(),
                prestoQueryExecutor, prestoConfig);

        PrestoMaterializedViewService materializedViewService = new PrestoMaterializedViewService(
                new PrestoConfig(),
                prestoQueryExecutor, metastore, queryMetadataStore);
        QueryExecutorService queryExecutorService = new QueryExecutorService(prestoQueryExecutor, metastore, materializedViewService, Clock.systemUTC(), '"');

        eventExplorer = new PrestoEventExplorer(new ProjectConfig(), queryExecutorService, metastore, continuousQueryService, materializedViewService);
        setupInline();
        super.setup();
//        new EventExplorerListener(new ProjectConfig(), null).onCreateCollection(new ProjectCreatedEvent(PROJECT_NAME));
        // todo find a better way of handling this
        Thread.sleep(15000);
    }

    @Override
    public EventStore getEventStore()
    {
        return testingPrestoEventStore;
    }

    public PrestoQueryExecutor getPrestoQueryExecutor()
    {
        return prestoQueryExecutor;
    }

    public PrestoContinuousQueryService getContinuousQueryService()
    {
        return continuousQueryService;
    }

    public JDBCPoolDataSource getMetastoreDataSource()
    {
        return metastoreDataSource;
    }

    public void setupInline()
    {
        AWSConfig awsConfig = testingEnvironment.getAWSConfig();

        int kinesisPort = getEnvironment().getKinesisPort();
        awsConfig
                .setKinesisEndpoint(kinesisPort == 0 ? null : "http://127.0.0.1:" + kinesisPort)
                .setEventStoreStreamName("rakam-events");

        testingPrestoEventStore = new AWSKinesisEventStore(
                awsConfig, getMetastore(),
                new FieldDependencyBuilder().build())
        {
            // KCL doesn't work with Kinesalite. See: https://github.com/mhart/kinesalite/issues/16
            @Override
            public int[] storeBatch(List<Event> events)
            {
                events.forEach(this::store);
                return SUCCESSFUL_BATCH;
            }
        };
    }

    @Override
    public Metastore getMetastore()
    {
        return metastore;
    }

    @Override
    public EventExplorer getEventExplorer()
    {
        return eventExplorer;
    }

    public TestingEnvironment getEnvironment()
    {
        return testingEnvironment;
    }
}
