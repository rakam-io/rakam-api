package org.rakam.analysis.rule.aggregation;

import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;

/**
 * Created by buremba on 16/01/14.
 */
public class MetricAggregationRule extends AggregationRule {
    public static final Analysis TYPE = Analysis.ANALYSIS_METRIC;

    public MetricAggregationRule(String projectId, AggregationType type) {
        super(projectId, type, null);
        if (type != AggregationType.COUNT) {
            throw new IllegalArgumentException("select parameter must be provided.");
        }
    }

    public MetricAggregationRule(String projectId, AggregationType type, FieldScript select) {
        super(projectId, type, select);
    }

    public MetricAggregationRule(String projectId, AggregationType type, FieldScript select, FilterScript filters) {
        super(projectId, type, select, filters);
    }

    public MetricAggregationRule(String projectId, AggregationType type, FieldScript select, FilterScript filters, FieldScript groupBy) {
        super(projectId, type, select, filters, groupBy);
    }

    public MetricAggregationRule() {
    }

    @Override
    public Analysis analysisType() {
        return TYPE;
    }
}
