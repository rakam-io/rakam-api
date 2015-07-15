package org.rakam.analysis.stream;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;

import java.util.Map;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/07/15 05:45.
 */
public class StreamPlanOptimizer extends PlanOptimizer {
    private final MetadataManager metadata;

    public StreamPlanOptimizer(MetadataManager metadataManager) {
        this.metadata = metadataManager;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator) {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan, null);
    }

    private class Rewriter
            extends PlanRewriter<Void>
    {
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context) {
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(metadata::getExactFunction)
                    .map(FunctionInfo::getAggregationFunction)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            if (!decomposable) {
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Decomposable aggregation queries are not supported.");
            }

            return context.defaultRewrite(new AggregationNode(
                    node.getId(),
                    node.getSource(),
                    node.getGroupBy(),
                    node.getAggregations(),
                    node.getFunctions(),
                    node.getMasks(),
                    PARTIAL,
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol()));
        }
    }

}
