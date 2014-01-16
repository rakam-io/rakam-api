package org.rakam.analysis.model;

import org.rakam.constant.Analysis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

/**
 * Created by buremba on 16/01/14.
 */
public class MetricAggregationRule extends AggregationRule implements Serializable {
    public MetricAggregationRule(UUID id) {
        super(id);
    }

    public MetricAggregationRule(UUID id, HashMap<String, String> filters) {
        super(id, filters);
    }

    public MetricAggregationRule(UUID id, HashMap<String, String> filters, String groupBy) {
        super(id, filters, groupBy);
    }

    public Analysis getType() {
        return Analysis.ANALYSIS_METRIC;
    }
}
