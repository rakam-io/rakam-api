package org.rakam.analysis.rule.aggregation;

import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;

/**
 * Created by buremba on 16/01/14.
 */
public class MetricAggregationRule extends AggregationRule {
    public static final Analysis TYPE = Analysis.ANALYSIS_METRIC;

    public MetricAggregationRule(String projectId, AggregationType type, FieldScript select) {
        super(projectId, type, select);
        this.id = buildId();
    }

    public MetricAggregationRule(String projectId, AggregationType type, FieldScript select, FilterScript filters) {
        super(projectId, type, select, filters);
        this.id = buildId();
    }

    public MetricAggregationRule(String projectId, AggregationType type, FieldScript select, FilterScript filters, FieldScript groupBy) {
        super(projectId, type, select, filters, groupBy);
        this.id = buildId();
    }

    public MetricAggregationRule() {

    }

    private String buildId() {
        return project+TYPE.id+type.id+(select==null ? "" : select)+(groupBy==null ? "" : groupBy)+(filters==null ? "" : filters);
    }

    @Override
    public Analysis analysisType() {
        return TYPE;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.METRIC_AGGREGATION_RULE;
    }
}
