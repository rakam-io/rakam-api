package org.rakam.analysis.model;

import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;

import java.util.HashMap;

/**
 * Created by buremba on 16/01/14.
 */
public class MetricAggregationRule extends AggregationRule {
    public static final Analysis TYPE = Analysis.ANALYSIS_METRIC;

    public MetricAggregationRule() {}

    public MetricAggregationRule(String projectId, AggregationType type) {
        super(projectId, type);
        if (type!=AggregationType.COUNT)
            throw new UnsupportedOperationException("'select' field must be specified if the AggregationType is not COUNT.");
    }

    public MetricAggregationRule(String projectId, AggregationType type, String select) {
        super(projectId, type, select);
    }

    public MetricAggregationRule(String projectId, AggregationType type, String select, HashMap<String, String> filters) {
        super(projectId, type, select, filters);
    }

    public MetricAggregationRule(String projectId, AggregationType type, String select, HashMap<String, String> filters, String groupBy) {
        super(projectId, type, select, filters, groupBy);
    }

    public String buildId(String project, AggregationType agg_type, String select, HashMap<String, String> filters, String groupBy) {
        //DigestUtils.md5Hex(  );
        return project+agg_type+select+groupBy+filters;
    }

    @Override
    public Analysis analysisType() {
        return TYPE;
    }

}
