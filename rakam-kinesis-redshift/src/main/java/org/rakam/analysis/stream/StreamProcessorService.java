package org.rakam.analysis.stream;

import com.facebook.presto.Session;
import com.facebook.presto.connector.system.SystemRecordSetProvider;
import com.facebook.presto.connector.system.SystemSplitManager;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConstructorFunction;
import org.rakam.plugin.ContinuousQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Locale.ENGLISH;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/07/15 15:17.
 */
public class StreamProcessorService implements ManagedService, RemoteService, MigrationAwareService {
    public static final String SERVICE_NAME = "rakam:hyperLogLogService";

    private final ConcurrentMap<String, Processor> containers = new ConcurrentHashMap<String, Processor>();

    private final ConstructorFunction<ContinuousQuery, Processor> constructorFunction =
            new ConstructorFunction<ContinuousQuery, Processor>() {
                public Processor createNew(ContinuousQuery report) {
                    SqlParser sqlParser = new SqlParser();
                    RakamMetadata connectorMetadata = new RakamMetadata();
                    connectorMetadata.setContext(new QueryAnalyzer.QueryContext().setColumns(null));
                    Statement statement = sqlParser.createStatement(report.query);

                    Session session = Session.builder()
                            .setUser("user")
                            .setSource("test")
                            .setCatalog("stream")
                            .setSchema("default")
                            .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                            .setLocale(ENGLISH)
                            .build();
                    MetadataManager metadataManager = new MetadataManager();
                    metadataManager.addConnectorMetadata("stream", "stream", connectorMetadata);
                    QueryExplainer explainer = new QueryExplainer(session, ImmutableList.of(), metadataManager, sqlParser, false);
                    Analyzer analyzer = new Analyzer(session, metadataManager, sqlParser, Optional.of(explainer), false);

                    Analysis analysis = analyzer.analyze(statement);
                    SplitManager splitManager = new SplitManager(new SystemSplitManager(new InMemoryNodeManager()));
                    splitManager.addConnectorSplitManager("stream", new QueryAnalyzer.MyConnectorSplitManager());
                    PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadataManager, sqlParser, splitManager, new IndexManager(), new FeaturesConfig(), true);
                    List<PlanOptimizer> planOptimizers = new ArrayList<>(planOptimizersFactory.get());
                    planOptimizers.add(new StreamPlanOptimizer(metadataManager));

                    PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
                    LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadataManager);
                    Plan plan = logicalPlanner.plan(analysis);
                    SubPlan subPlans = new PlanFragmenter().createSubPlans(plan);
                    PlanFragment fragment =  subPlans.getFragment();

                    PageSourceManager pageSourceManager = new PageSourceManager(new SystemRecordSetProvider());

                    pageSourceManager.addConnectorPageSourceProvider("stream", new ConnectorPageSourceProvider() {
                        @Override
                        public ConnectorPageSource createPageSource(ConnectorSplit split, List<ConnectorColumnHandle> columns) {
                            return null;
                        }
                    });

                    return fragment.getRoot().accept(new StreamQueryExecutionPlanVisitor(metadataManager, fragment.getSymbols()), null);
                }
            };
    private NodeEngine nodeEngine;


    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return null;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void clearPartitionReplica(int partitionId) {

    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return null;
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }
}
