package org.rakam.cache.hazelcast;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.AverageCounter;
import org.rakam.cache.hazelcast.models.SimpleCounter;
import org.rakam.cache.hazelcast.operations.*;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 01:23.
 */
public class RakamDataSerializableFactory implements DataSerializableFactory {
    public static final int F_ID = 1;

    public static final int COUNTER_INCREMENT_OPERATION = 1;
    public static final int COUNTER = 2;
    public static final int AVERAGE_COUNTER = 3;
    public static final int AVERAGE_COUNTER_INCREMENT_OPERATION = 4;
    public static final int LIMIT_PREDICATE = 5;
    public static final int ALL_STRING_SET_OPERATION = 6;
    public static final int ADD_ALL_STRING_SET_OPERATION = 7;
    public static final int METRIC_AGGREGATION_RULE = 8;
    public static final int TIMESERIES_AGGREGATION_RULE = 9;
    public static final int GET_STRING_COUNTS_OPERATION = 10;
    public static final int COUNTER_SET_OPERATION = 10;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == COUNTER_INCREMENT_OPERATION) {
            return new CounterIncrementOperation();
        } else if (typeId == COUNTER) {
            return new SimpleCounter();
        } else if (typeId == AVERAGE_COUNTER) {
            return new AverageCounter();
        } else if (typeId == AVERAGE_COUNTER_INCREMENT_OPERATION) {
            return new AverageCounterIncrementOperation();
        } else if (typeId == LIMIT_PREDICATE) {
            return new LimitPredicate();
        } else if (typeId == ALL_STRING_SET_OPERATION) {
            return new AddStringSetOperation();
        }else if (typeId == ADD_ALL_STRING_SET_OPERATION) {
            return new AddAllStringSetOperation();
        }else if (typeId == METRIC_AGGREGATION_RULE) {
            return new MetricAggregationRule();
        }else if (typeId == TIMESERIES_AGGREGATION_RULE) {
            return new MetricAggregationRule();
        }else if (typeId == GET_STRING_COUNTS_OPERATION) {
            return new GetStringCountsOperation();
        }else if (typeId == COUNTER_SET_OPERATION) {
            return new CounterSetOperation();
        } else {
            return null;
        }
    }
}

