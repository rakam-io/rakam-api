package org.rakam.integration.java;

import org.junit.Test;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.query.simple.SimpleFilterScript;
import org.rakam.analysis.query.simple.predicate.FilterPredicates;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.constant.AggregationType;
import org.rakam.model.Entry;
import org.rakam.util.SpanTime;

import java.util.HashSet;
import java.util.function.Predicate;

/**
 * Created by buremba on 19/01/14.
 */
public class AggregationRuleTest {

    @Test
    public void testAdding() {
        HashSet<AnalysisRule> aggs = new HashSet<>();
        String projectId = "e74607921dad4803b998";
        aggs.add(new MetricAggregationRule(projectId, AggregationType.COUNT_X, new SimpleFieldScript("test")));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.SUM_X, new SimpleFieldScript("test")));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.MAXIMUM_X, new SimpleFieldScript("test")));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.UNIQUE_X, SpanTime.fromString("1min").period,  new SimpleFieldScript("baska"), null,  new SimpleFieldScript("referral")));

        Predicate<Entry> eq = FilterPredicates.eq("ali", "veli");
        aggs.add(new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, new SimpleFieldScript("test"), new SimpleFilterScript(eq, false)));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT_X, SpanTime.fromString("1min").period,  new SimpleFieldScript("referral")));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT, SpanTime.fromString("1min").period, null, null));

        // tracker_id -> aggregation rules
        //aggregation_map.put("e74607921dad4803b998", aggs);
    }
}
