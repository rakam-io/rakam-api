package org.rakam.analysis.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.util.SpanDateTime;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by buremba on 16/01/14.
 */
public class TimeSeriesAggregationRule extends AggregationRule {
    public static final Analysis TYPE = Analysis.ANALYSIS_TIMESERIES;
    public SpanDateTime interval;

    public TimeSeriesAggregationRule() {}

    public TimeSeriesAggregationRule(String projectId, AggregationType type, SpanDateTime interval, String select) {
        super(projectId, type, select);
        this.interval = interval;
    }

    public TimeSeriesAggregationRule(String projectId, AggregationType type, SpanDateTime interval, String select, HashMap<String, String> filters) {
        super(projectId, type, select, filters);
        this.interval = interval;
    }

    public TimeSeriesAggregationRule(String projectId, AggregationType type, SpanDateTime interval, String select, HashMap<String, String> filters, String groupBy) {
        super(projectId, type, select, filters, groupBy);
        this.interval = interval;
    }

    @Override
    public Analysis analysisType() {
        return TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        interval.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        interval = new SpanDateTime();
        interval.readData(in);
    }

    public String buildId(String project, AggregationType agg_type, String select, HashMap<String, String> filters, String groupBy) {
        //DigestUtils.md5Hex(  );
        return project+agg_type+select+groupBy+filters;
    }
}
