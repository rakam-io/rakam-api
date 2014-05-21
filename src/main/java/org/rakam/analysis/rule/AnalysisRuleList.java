package org.rakam.analysis.rule;

import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.constant.AggregationType;

import java.io.Serializable;
import java.util.HashSet;

/**
 * Created by buremba on 17/01/14.
 */
public class AnalysisRuleList extends HashSet<AnalysisRule> implements Serializable {
    @Override
    public boolean add(AnalysisRule rule) {
        if (rule instanceof AggregationRule) {
            AggregationRule aggRule = (AggregationRule) rule;

            if (aggRule.type == AggregationType.AVERAGE_X) {
                switch (rule.analysisType()) {
                    case ANALYSIS_METRIC:
                        super.add(new MetricAggregationRule(aggRule.project, AggregationType.COUNT_X, aggRule.select, aggRule.filters, aggRule.groupBy));
                        break;
                    case ANALYSIS_TIMESERIES:
                        super.add(new TimeSeriesAggregationRule(aggRule.project, AggregationType.COUNT, ((TimeSeriesAggregationRule) rule).interval, aggRule.select, aggRule.filters, aggRule.groupBy));
                        break;
                }
            }
        }
        return super.add(rule);
    }


}
