package org.rakam.analysis.model;

import org.rakam.constant.Analysis;
import org.rakam.util.SpanDateTime;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

/**
 * Created by buremba on 16/01/14.
 */
public class TimeSeriesAggregationRule extends AggregationRule implements Serializable {
    public final SpanDateTime interval;

    public TimeSeriesAggregationRule(UUID id, SpanDateTime interval) {
        super(id);
        this.interval = interval;
    }

    public TimeSeriesAggregationRule(UUID id, SpanDateTime interval, HashMap<String, String> filters) {
        super(id, filters);
        this.interval = interval;
    }

    public TimeSeriesAggregationRule(UUID id, SpanDateTime interval, HashMap<String, String> filters, String groupBy) {
        super(id, filters, groupBy);
        this.interval = interval;
    }

    public Analysis getType() {
        return Analysis.ANALYSIS_TIMESERIES;
    }
}
