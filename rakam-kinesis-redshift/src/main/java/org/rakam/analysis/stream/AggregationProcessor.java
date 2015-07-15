package org.rakam.analysis.stream;

import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.rakam.collection.SchemaField;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;

/**
* Created by buremba <Burak Emre Kabakcı> on 14/07/15 15:59.
*/
public class AggregationProcessor implements Processor {
    private final List<Accumulator> accumulators;
    private final List<String> columns;

    public AggregationProcessor(List<String> columns, List<AccumulatorFactory> accumulatorFactories) {
        accumulators = accumulatorFactories.stream()
                .map(e -> e.createAccumulator()).collect(Collectors.toList());
        this.columns = columns;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");

        for (int i = 0; i < accumulators.size(); i++) {
            accumulators.get(i).addIntermediate(page.getBlock(i));
        }
    }

    @Override
    public Iterator<Page> getOutput()
    {
        // project results into output blocks
        List<Type> types = accumulators.stream().map(Accumulator::getFinalType).collect(toImmutableList());

        PageBuilder pageBuilder = new PageBuilder(types);

        pageBuilder.declarePosition();
        for (int i = 0; i < types.size(); i++) {
            Accumulator aggregator = accumulators.get(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            aggregator.evaluateFinal(blockBuilder);
        }

        return Arrays.asList(pageBuilder.build()).iterator();
    }

    @Override
    public List<SchemaField> getSchema() {
        ImmutableList.Builder<SchemaField> builder = ImmutableList.builder();
        for (int i = 0; i < accumulators.size(); i++) {
            builder.add(new SchemaField(columns.get(i), RakamMetadata.convertColumn(accumulators.get(i).getFinalType()), false));
        }
        return builder.build();
    }

}
