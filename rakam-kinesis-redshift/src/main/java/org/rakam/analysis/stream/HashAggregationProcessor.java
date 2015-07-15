package org.rakam.analysis.stream;

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.MemoryManager;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.rakam.collection.SchemaField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
* Created by buremba <Burak Emre Kabakcı> on 14/07/15 15:59.
*/
public class HashAggregationProcessor implements Processor {

    private final GroupByHash groupByHash;
    private final MemoryManager memoryManager;
    private final List<GroupedAccumulator> aggregators;
    private final List<SchemaField> columns;
    private final List<Integer> channels;


    public HashAggregationProcessor(List<String> columnNames, List<AccumulatorFactory> accumulatorFactories, List<Type> groupByTypes, List<Integer> groupByChannels, Optional<Integer> hashChannel, MemoryManager memoryManager, Map<Symbol, Integer> outputMappings, List<Symbol> groupBySymbols, List<Symbol> aggregationOutputSymbols) {
        this.groupByHash = new GroupByHash(groupByTypes, Ints.toArray(groupByChannels), hashChannel, 10_000);
        this.memoryManager = memoryManager;

        // wrapper each function with an aggregator
        ImmutableList.Builder<GroupedAccumulator> builder = ImmutableList.builder();
        checkNotNull(accumulatorFactories, "accumulatorFactories is null");
        for (int i = 0; i < accumulatorFactories.size(); i++) {
            AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
            builder.add(accumulatorFactory.createGroupedIntermediateAccumulator());
        }
        this.aggregators = builder.build();
        this.channels = aggregationOutputSymbols.stream()
                .map(symbol -> outputMappings.get(symbol))
                .collect(Collectors.toList());

        this.columns = buildSchema(outputMappings, groupBySymbols, aggregationOutputSymbols, columnNames);
    }

    private List<SchemaField> buildSchema(Map<Symbol, Integer> outputMappings, List<Symbol> groupBySymbols, List<Symbol> aggregationOutputSymbols, List<String> columnNames) {
        SchemaField[] schemaFields = new SchemaField[columnNames.size()];
        for (Symbol groupBySymbol : groupBySymbols) {
            Integer idx = outputMappings.get(groupBySymbol);
            schemaFields[idx] = new SchemaField(columnNames.get(idx), RakamMetadata.convertColumn(groupByHash.getTypes().get(idx)), false);
        }
        for (int i = 0; i < aggregationOutputSymbols.size(); i++) {
            Integer idx = outputMappings.get(aggregationOutputSymbols.get(i));
            schemaFields[idx] = new SchemaField(columnNames.get(idx), RakamMetadata.convertColumn(aggregators.get(i).getFinalType()), false);
        }
        return Arrays.asList(schemaFields);
    }

    @Override
    public void addInput(Page page) {
        GroupByIdBlock groupIds = groupByHash.getGroupIds(page);

        for (int i = 0; i < aggregators.size(); i++) {
            aggregators.get(i).addIntermediate(groupIds, page.getBlock(channels.get(i)));
        }
    }

    public boolean isFull() {
        long memorySize = groupByHash.getEstimatedSize();
        for (GroupedAccumulator aggregator : aggregators) {
            memorySize += aggregator.getEstimatedSize();
        }
        return !memoryManager.canUse(memorySize);
    }

    @Override
    public Iterator<Page> getOutput() {
        List<Type> types = new ArrayList<>(groupByHash.getTypes());
        types.addAll(aggregators.stream().map(GroupedAccumulator::getFinalType)
                .collect(Collectors.toList()));

        final PageBuilder pageBuilder = new PageBuilder(types);

        return new AbstractIterator<Page>() {
            private final int groupCount = groupByHash.getGroupCount();
            private int groupId;

            @Override
            protected Page computeNext() {
                if (groupId >= groupCount) {
                    return endOfData();
                }

                pageBuilder.reset();

                List<Type> types = groupByHash.getTypes();
                while (!pageBuilder.isFull() && groupId < groupCount) {
                    groupByHash.appendValuesTo(groupId, pageBuilder, 0);

                    pageBuilder.declarePosition();
                    for (int i = 0; i < aggregators.size(); i++) {
                        GroupedAccumulator aggregator = aggregators.get(i);
                        BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                        aggregator.evaluateFinal(groupId, output);
                    }

                    groupId++;
                }

                return pageBuilder.build();
            }
        };
    }

    @Override
    public List<SchemaField> getSchema() {
        return columns;
    }
}
