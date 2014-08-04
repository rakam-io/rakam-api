package org.rakam.analysis;

import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.analysis.script.mvel.MvelFieldScript;
import org.rakam.analysis.script.mvel.MvelFilterScript;
import org.rakam.analysis.script.simple.SimpleFieldScript;
import org.rakam.analysis.script.simple.SimpleFilterScript;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.util.SpanTime;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 15/01/14.
 */
public class AnalysisRuleParser {

    public static AnalysisRule parse(JsonObject json) throws IllegalArgumentException {
        AnalysisRule rule;
        String project = json.getString("project");
        if (json.getString("analysis") == null)
            throw new IllegalArgumentException("analysis type is required.");
        Analysis analysisType;
        try {
            analysisType = Analysis.get(json.getString("analysis"));
        } catch (IllegalArgumentException e) {
            throw new IllegalAccessError("analysis type does not exist.");
        }
        if (project == null)
            throw new IllegalArgumentException("project id is required.");

        if (analysisType == Analysis.ANALYSIS_TIMESERIES || analysisType == Analysis.ANALYSIS_METRIC) {
            FilterScript filter = getFilter(json.getObject("filter"));
            FieldScript groupBy = getField(json.getField("group_by"));
            FieldScript select = getField(json.getField("select"));
            if (json.getString("aggregation") == null)
                throw new IllegalArgumentException("aggregation type is required.");
            AggregationType aggType;
            try {
                aggType = AggregationType.get(json.getString("aggregation"));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("aggregation type does not exist.");
            }
            if (aggType == null)
                throw new IllegalArgumentException("aggregation type is required.");
            if (aggType != AggregationType.COUNT && select == null)
                throw new IllegalArgumentException("select attribute is required if aggregation type is not COUNT.");

            if (groupBy != null && select == null)
                select = groupBy;

            if (analysisType == Analysis.ANALYSIS_TIMESERIES) {
                String interval = json.getString("interval");
                if (interval == null)
                    throw new IllegalArgumentException("interval is required for time-series.");
                rule = new TimeSeriesAggregationRule(project, aggType, SpanTime.fromPeriod(interval), select, filter, groupBy);
            } else if (analysisType == Analysis.ANALYSIS_METRIC) {
                rule = new MetricAggregationRule(project, aggType, select, filter, groupBy);
            } else {
                throw new IllegalStateException("aggregation analysis type couldn't identified");
            }
        } else {
            throw new IllegalStateException("analysis type couldn't identified");
        }
        String strategy = json.getString("strategy");
        if (strategy != null)
            try {
                rule.strategy = AnalysisRuleStrategy.get(strategy);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("strategy couldn't identified.");
            }
        return rule;
    }

    public static FieldScript getField(Object field) {
        if (field != null) {
            if (field instanceof JsonObject) {
                String script = ((JsonObject) field).getString("script");
                if (script != null)
                    return new MvelFieldScript(script);
            } else if (field instanceof String) {
                return new SimpleFieldScript((String) field);
            }
        }
        return null;
    }

    public static FilterScript getFilter(JsonObject field) {
        if (field != null) {
            String script = field.getString("script");
            if (script != null)
                return new MvelFilterScript(script);
            else
                return new SimpleFilterScript(field.toMap());
        }

        return null;
    }
}
