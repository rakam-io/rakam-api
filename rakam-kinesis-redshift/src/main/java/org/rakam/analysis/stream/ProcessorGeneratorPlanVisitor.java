package org.rakam.analysis.stream;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.stream.processor.AggregationProcessor;
import org.rakam.analysis.stream.processor.HashAggregationProcessor;
import org.rakam.analysis.stream.processor.Processor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/07/15 12:57.
 */
public class ProcessorGeneratorPlanVisitor
        extends PlanVisitor<Void, Processor>
{

    private final Metadata metadata;
    private final Map<Symbol, Type> types;
    private List<String> columnNames;

    public ProcessorGeneratorPlanVisitor(Metadata metadata, Map<Symbol, Type> types)
    {
        this.metadata = metadata;
        this.types = types;
    }

    @Override
    public Processor visitOutput(OutputNode node, Void context) {
        checkArgument(node.getSource() instanceof AggregationNode, "The query must be aggregation query.");
        this.columnNames = node.getColumnNames();
        return node.getSource().accept(this, context);
    }

    private Processor planGlobalAggregation(AggregationNode node)
    {
        int outputChannel = 0;
        ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
        List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
        for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
            Symbol symbol = entry.getKey();

            accumulatorFactories.add(buildAccumulatorFactory(makeLayout(node), node.getFunctions().get(symbol), entry.getValue(), node.getMasks().get(entry.getKey()), node.getSampleWeight(), node.getConfidence()));
            outputMappings.put(symbol, outputChannel); // one aggregation per channel
            outputChannel++;
        }
        return new AggregationProcessor(columnNames, accumulatorFactories);
    }

    @Override
    public Processor visitAggregation(AggregationNode node, Void context)
    {

        if (node.getGroupBy().isEmpty()) {
            return planGlobalAggregation(node);
        }

        return planGroupByAggregation(node);
    }

    private AccumulatorFactory buildAccumulatorFactory(ImmutableMap<Symbol, Integer> layout, Signature function, FunctionCall call, @Nullable Symbol mask, Optional<Symbol> sampleWeight, double confidence)
    {
        List<Integer> arguments = new ArrayList<>();
        for (Expression argument : call.getArguments()) {
            Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
            arguments.add(layout.get(argumentSymbol));
        }

        Optional<Integer> maskChannel = Optional.empty();

        if (mask != null) {
            maskChannel = Optional.of(layout.get(mask));
        }

        Optional<Integer> sampleWeightChannel = Optional.empty();
        if (sampleWeight.isPresent()) {
            sampleWeightChannel = Optional.of(layout.get(sampleWeight.get()));
        }

        return metadata.getExactFunction(function).getAggregationFunction().bind(arguments, maskChannel, sampleWeightChannel, confidence);
    }

    private ImmutableMap<Symbol, Integer> makeLayout(PlanNode node)
    {
        ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
        int channel = 0;
        for (Symbol symbol : node.getOutputSymbols()) {
            outputMappings.put(symbol, channel);
            channel++;
        }
        return outputMappings.build();
    }

    private HashAggregationProcessor planGroupByAggregation(AggregationNode node)
    {
        List<Symbol> groupBySymbols = node.getGroupBy();

        List<Symbol> aggregationOutputSymbols = new ArrayList<>();
        List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
        for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
            Symbol symbol = entry.getKey();

            accumulatorFactories.add(buildAccumulatorFactory(makeLayout(node), node.getFunctions().get(symbol), entry.getValue(), node.getMasks().get(entry.getKey()), node.getSampleWeight(), node.getConfidence()));
            aggregationOutputSymbols.add(symbol);
        }

        ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
        // add group-by key fields each in a separate channel
        int channel = 0;
        for (Symbol symbol : groupBySymbols) {
            outputMappings.put(symbol, channel);
            channel++;
        }

        // hashChannel follows the group by channels
        if (node.getHashSymbol().isPresent()) {
            outputMappings.put(node.getHashSymbol().get(), channel++);
        }

        // aggregations go in following channels
        for (Symbol symbol : aggregationOutputSymbols) {
            outputMappings.put(symbol, channel);
            channel++;
        }

        List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, makeLayout(node));
        List<Type> groupByTypes = groupBySymbols.stream()
                .map(entry -> types.get(entry))
                .collect(toImmutableList());

        Optional<Integer> hashChannel = node.getHashSymbol().map(input -> makeLayout(node).get(input));

        Map<Symbol, Integer> build = outputMappings.build();
        return new HashAggregationProcessor(columnNames, accumulatorFactories, groupByTypes, groupByChannels, hashChannel, null, build, groupBySymbols, aggregationOutputSymbols);
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol));
        }
        return builder.build();
    }
}


