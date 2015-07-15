package org.rakam.analysis.stream;

import com.facebook.presto.Session;
import com.facebook.presto.connector.system.SystemRecordSetProvider;
import com.facebook.presto.connector.system.SystemSplitManager;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
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
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.SchemaField;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

/**
 * Created by buremba <Burak Emre Kabakcı> on 07/07/15 02:08.
 */
public class QueryAnalyzer
{
    private final static Session session = Session.builder()
            .setUser("user")
            .setSource("test")
            .setCatalog("stream")
            .setSchema("default")
            .setTimeZoneKey(TimeZoneKey.UTC_KEY)
            .setLocale(ENGLISH)
            .build();

    private final SqlParser sqlParser = new SqlParser();
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    private final MetadataManager metadataManager = new MetadataManager();
    private final RakamMetadata connectorMetadata = new RakamMetadata();
    private final QueryExplainer explainer = new QueryExplainer(session, ImmutableList.of(), metadataManager, sqlParser, false);
    private final Analyzer analyzer = new Analyzer(session, metadataManager, sqlParser, Optional.of(explainer), false);
    private final SplitManager splitManager = new SplitManager(new SystemSplitManager(new InMemoryNodeManager()));
    private final PlanOptimizersFactory planOptimizersFactory;
    private final LogicalPlanner logicalPlanner;
    private final ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadataManager);
    private final IndexJoinLookupStats indexJoinLookupStats = new IndexJoinLookupStats();;
    private final CompilerConfig compilerConfig = new CompilerConfig();
    private final TaskManagerConfig taskManagerConfig = new TaskManagerConfig();;

    public QueryAnalyzer()
    {
        this.metadataManager.addConnectorMetadata("stream", "stream", connectorMetadata);
        this.splitManager.addConnectorSplitManager("stream", new MyConnectorSplitManager());
        this.planOptimizersFactory =
                new PlanOptimizersFactory(metadataManager, sqlParser, splitManager, new IndexManager(), new FeaturesConfig(), true);
        List<PlanOptimizer> planOptimizers = new ArrayList<>(planOptimizersFactory.get());
        planOptimizers.add(new StreamPlanOptimizer(metadataManager));
        this.logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadataManager);
    }

    public Session getSession() {
        return session;
    }

    public ContinuousQueryExecutor planPartial(String sql, List<GenericRecord> records, List<SchemaField> columns, Consumer<Page> sink) {
        LocalExecutionPlanner.LocalExecutionPlan plan1 = plan(sql, (split, cols) -> {
            checkNotNull(cols, "columns is null");
            return new RecordPageSource(new MyRecordSet(cols, records));
        }, columns, (operatorId, sourceType) ->
                new SinkOperatorFactory(sourceType, operatorId, sink));
        return getExecutor(plan1);
    }

    public ContinuousQueryExecutor getExecutor(LocalExecutionPlanner.LocalExecutionPlan plan1) {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Executor executor = command -> executorService.execute(command);
        PipelineContext pipelineContext = new PipelineContext(new TaskContext(new TaskId("0"), executor, session), executor, false, false);
        List<Driver> collect = plan1.getDriverFactories().stream()
                .map(x -> x.createDriver(pipelineContext.addDriverContext())).collect(Collectors.toList());

        return new ContinuousQueryExecutor(collect);
    }

    public synchronized LocalExecutionPlanner.LocalExecutionPlan plan(String sql, ConnectorPageSourceProvider connectorPageSourceProvider, List<SchemaField> columns, OutputFactory outputFactory) {
        connectorMetadata.setContext(new QueryContext().setColumns(columns));
        Statement statement = sqlParser.createStatement(sql);
        Analysis analysis = analyzer.analyze(statement);

        Plan plan = logicalPlanner.plan(analysis);
        SubPlan subPlans = new PlanFragmenter().createSubPlans(plan);
        PlanFragment fragment =  subPlans.getFragment();

        PageSourceManager pageSourceManager = new PageSourceManager(new SystemRecordSetProvider());

        pageSourceManager.addConnectorPageSourceProvider("stream", connectorPageSourceProvider);

        final PageSinkManager pageSinkManager = new PageSinkManager();

        LocalExecutionPlanner localExecutionPlanner = new LocalExecutionPlanner(
                metadataManager,
                sqlParser,
                pageSourceManager,
                new IndexManager(),
                pageSinkManager,
                () -> {
                    throw new UnsupportedOperationException();
                },
                expressionCompiler, indexJoinLookupStats, compilerConfig, taskManagerConfig);

        return localExecutionPlanner
                .plan(session, fragment.getRoot(), fragment.getOutputLayout(), fragment.getSymbols(), outputFactory);
    }

    public synchronized ContinuousQueryExecutor execute(String sql, ConnectorPageSourceProvider connectorPageSourceProvider, List<SchemaField> columns, OutputFactory outputFactory) {
        connectorMetadata.setContext(new QueryContext().setColumns(columns));
        Statement statement = sqlParser.createStatement(sql);
        Analysis analysis = analyzer.analyze(statement);

        Plan plan = logicalPlanner.plan(analysis);
        SubPlan subPlans = new PlanFragmenter().createSubPlans(plan);
        PlanFragment fragment =  subPlans.getFragment();

        PageSourceManager pageSourceManager = new PageSourceManager(new SystemRecordSetProvider());

        pageSourceManager.addConnectorPageSourceProvider("stream", connectorPageSourceProvider);

        final PageSinkManager pageSinkManager = new PageSinkManager();
        com.facebook.presto.sql.planner.LocalExecutionPlanner localExecutionPlanner = new com.facebook.presto.sql.planner.LocalExecutionPlanner(
                metadataManager,
                sqlParser,
                pageSourceManager,
                new IndexManager(),
                pageSinkManager,
                () -> { throw new UnsupportedOperationException(); },
                expressionCompiler, indexJoinLookupStats, compilerConfig, taskManagerConfig);

        com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan plan1 = localExecutionPlanner
                .plan(session, fragment.getRoot(), fragment.getOutputLayout(), fragment.getSymbols(), outputFactory);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Executor executor = command -> executorService.execute(command);
        PipelineContext pipelineContext = new PipelineContext(new TaskContext(new TaskId("0"), executor, session), executor, false, false);
        List<Driver> collect = plan1.getDriverFactories().stream()
                .map(x -> x.createDriver(pipelineContext.addDriverContext())).collect(Collectors.toList());

        return new ContinuousQueryExecutor(collect);
    }

    public Metadata getMetadata() {
        return metadataManager;
    }

    public static class MyConnectorSplitManager implements ConnectorSplitManager {
        @Override
        public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ConnectorColumnHandle> tupleDomain) {
            return new ConnectorPartitionResult(ImmutableList.of(new ConnectorPartition() {
                @Override
                public String getPartitionId() {
                    return "";
                }

                @Override
                public TupleDomain<ConnectorColumnHandle> getTupleDomain() {
                    return TupleDomain.all();
                }
            }), TupleDomain.all());
        }

        @Override
        public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions) {
            return null;
        }
    }

    public static class QueryContext {
        private List<SchemaField> columns;

        public List<SchemaField> getColumns() {
            return columns;
        }

        public QueryContext setColumns(List<SchemaField> columns) {
            this.columns = columns;
            return this;
        }
    }

}
