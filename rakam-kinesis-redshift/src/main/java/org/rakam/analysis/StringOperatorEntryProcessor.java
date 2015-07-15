package org.rakam.analysis;

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
import com.facebook.presto.spi.Page;
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
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.AbstractEntryProcessor;
import org.rakam.analysis.stream.Processor;
import org.rakam.analysis.stream.QueryAnalyzer;
import org.rakam.analysis.stream.RakamMetadata;
import org.rakam.analysis.stream.StreamQueryExecutionPlanVisitor;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ENGLISH;

/**
* Created by buremba <Burak Emre Kabakcı> on 13/07/15 14:21.
*/
class StringOperatorEntryProcessor extends AbstractEntryProcessor<String, Processor> implements HazelcastInstanceAware {
    private final ContinuousQuery report;
    private final List<SchemaField> commonColumns;
    private final Page page;
    private HazelcastInstance hazelcastInstance;

    public StringOperatorEntryProcessor(ContinuousQuery report, List<SchemaField> commonColumns, Page page) {
        super(true);
        this.report = report;
        this.commonColumns = commonColumns;
        this.page = page;
    }

    @Override
    public Object process(Map.Entry<String, Processor> entry) {
        Processor op = entry.getValue();
        if (op == null) {
            // it's expensive to create StreamQueryAnalyzer instance so we cache it in local node.
            StreamQueryAnalyzer queryParser = (StreamQueryAnalyzer) hazelcastInstance.getUserContext()
                    .computeIfAbsent("queryParser", (key) -> new StreamQueryAnalyzer());
            op = queryParser.parse(report, commonColumns);
            entry.setValue(op);
        }
        op.addInput(page);
        return null;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }


    private static class StreamQueryAnalyzer {
        SqlParser sqlParser = new SqlParser();
        RakamMetadata connectorMetadata = new RakamMetadata();
        Session session = Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("stream")
                .setSchema("default")
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLocale(ENGLISH)
                .build();
        MetadataManager metadataManager = new MetadataManager();
        QueryExplainer explainer = new QueryExplainer(session, ImmutableList.of(), metadataManager, sqlParser, false);
        Analyzer analyzer = new Analyzer(session, metadataManager, sqlParser, Optional.of(explainer), false);
        SplitManager splitManager = new SplitManager(new SystemSplitManager(new InMemoryNodeManager()));
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadataManager, sqlParser, splitManager, new IndexManager(), new FeaturesConfig(), true);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizersFactory.get(), idAllocator, metadataManager);

        public StreamQueryAnalyzer() {
            metadataManager.addConnectorMetadata("stream", "stream", connectorMetadata);
            splitManager.addConnectorSplitManager("stream", new QueryAnalyzer.MyConnectorSplitManager());
        }

        public synchronized Processor parse(ContinuousQuery report, List<SchemaField> commonColumns) {
            connectorMetadata.setContext(new QueryAnalyzer.QueryContext().setColumns(commonColumns));
            Statement statement = sqlParser.createStatement(report.query);
            Analysis analysis = analyzer.analyze(statement);
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
    }
}
