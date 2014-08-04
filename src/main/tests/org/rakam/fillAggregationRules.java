package org.rakam;

import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.analysis.script.simple.SimpleFieldScript;
import org.rakam.constant.AggregationType;
import org.rakam.util.SpanTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by buremba on 04/05/14.
 */
public class fillAggregationRules {
    public static void fill(Map<String, AnalysisRuleList> aggregation_map) {
        AnalysisRuleList aggs = new AnalysisRuleList();
        String projectId = "e74607921dad4803b998";
        aggs.add(new MetricAggregationRule(projectId, AggregationType.COUNT_X, new SimpleFieldScript("test")));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.SUM_X, new SimpleFieldScript("test")));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.MAXIMUM_X, new SimpleFieldScript("test")));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.UNIQUE_X, SpanTime.fromPeriod("1min"),  new SimpleFieldScript("baska"), null,  new SimpleFieldScript("referral")));

        HashMap<String, Object> a = new HashMap();
        a.put("a", "a");
        //aggs.add(new MetricAggregationRule(projectId, AggregationType.AVERAGE_X,  new SimpleFieldScript("test"), new SimpleFilterScript(a)));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT_X, SpanTime.fromPeriod("1min"),  new SimpleFieldScript("referral")));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT, SpanTime.fromPeriod("1min"), null, null));

        // tracker_id -> aggregation rules
        aggregation_map.put("e74607921dad4803b998", aggs);
    }
}
