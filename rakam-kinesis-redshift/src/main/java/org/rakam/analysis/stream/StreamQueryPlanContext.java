package org.rakam.analysis.stream;

import com.facebook.presto.operator.aggregation.AccumulatorFactory;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/07/15 13:29.
 */
public class StreamQueryPlanContext {
    private List<AccumulatorFactory> accumulatorFactories;

    public void setAccumulatorFactories(List<AccumulatorFactory> accumulatorFactories) {
        this.accumulatorFactories = accumulatorFactories;
    }
}
