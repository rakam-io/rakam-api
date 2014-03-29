package org.rakam.analysis;

import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.MetricAggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.constant.Analysis;

/**
 * Created by buremba on 17/01/14.
 */

public class AggregationRuleFactory {
    public static AggregationRule create(int typeId) {
        switch (Analysis.get(typeId)) {
            case ANALYSIS_METRIC:
                return new MetricAggregationRule();
            case ANALYSIS_TIMESERIES:
                return new TimeSeriesAggregationRule();
            default:
                return null;
        }
    }
}