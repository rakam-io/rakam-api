package org.rakam.analysis.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.analysis.AggregationRuleFactory;
import org.rakam.analysis.AggregationRuleListFactory;
import org.rakam.constant.AggregationType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by buremba on 17/01/14.
 */
public class AggregationRuleList extends HashSet<AggregationRule> implements com.hazelcast.nio.serialization.IdentifiedDataSerializable {
    public static final int ID = 1;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeShort(size());
        Iterator<AggregationRule> it = this.iterator();
        while(it.hasNext()) {
            AggregationRule agg = it.next();
            out.writeShort(agg.analysisType().id);
            agg.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        short size = in.readShort();
        for(int i=0; i<size; i++) {
            AggregationRule rule = new AggregationRuleFactory().create(in.readShort());
            rule.readData(in);
            add(rule);
        }
    }

    @Override
    public int getFactoryId() {
        return AggregationRuleListFactory.ID;
    }

    @Override
    public int getId() {
        return ID;
    }

    @Override
    public boolean add(AggregationRule rule) {
        if(rule.type == AggregationType.AVERAGE_X) {
            switch(rule.analysisType()) {
                case ANALYSIS_METRIC:
                    super.add(new MetricAggregationRule(rule.project, AggregationType.COUNT_X, rule.select, rule.filters, rule.groupBy));
                    break;
                case ANALYSIS_TIMESERIES:
                    super.add(new TimeSeriesAggregationRule(rule.project, AggregationType.COUNT, ((TimeSeriesAggregationRule) rule).interval, rule.select, rule.filters, rule.groupBy));
                    break;
            }
        }

        return super.add(rule);
    }



}
