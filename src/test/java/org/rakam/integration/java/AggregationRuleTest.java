package org.rakam.integration.java;

import org.junit.Test;
import org.rakam.analysis.model.AggregationRuleList;
import org.rakam.analysis.model.MetricAggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.constant.AggregationType;
import org.rakam.util.SpanDateTime;

import java.util.HashMap;

/**
 * Created by buremba on 19/01/14.
 */
public class AggregationRuleTest {

    @Test
    public void testAdding() {
        AggregationRuleList aggs = new AggregationRuleList();
        String projectId = "e74607921dad4803b998";
        aggs.add(new MetricAggregationRule(projectId, AggregationType.COUNT));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.COUNT));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.SUM_X, "test"));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.MAXIMUM_X, "test"));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, "test"));

        HashMap<String, String> a = new HashMap();
        a.put("a", "a");
        aggs.add(new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, "test", a));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT, SpanDateTime.fromPeriod("1min"), null));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT, SpanDateTime.fromPeriod("1min"), null, null, "a"));

    }
}
