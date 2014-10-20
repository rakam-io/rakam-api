package org.rakam.analysis;

import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.query.mvel.MVELFieldScript;
import org.rakam.analysis.query.mvel.MVELFilterScript;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.query.simple.SimpleFilterScript;
import org.rakam.analysis.query.simple.predicate.FilterPredicates;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.util.Interval;
import org.rakam.util.Tuple;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.util.function.Predicate;

/**
 * Created by buremba on 15/01/14.
 */
public class AnalysisRuleParser {

    public static AnalysisRule parse(JsonObject json) throws IllegalArgumentException {
        AnalysisRule rule;
        String project = json.getString("tracking");
        if (json.getString("analysis") == null)
            throw new IllegalArgumentException("analysis type is required.");
        Analysis analysisType;
        try {
            analysisType = Analysis.get(json.getString("analysis"));
        } catch (IllegalArgumentException e) {
            throw new IllegalAccessError("analysis type does not exist.");
        }
        if (project == null)
            throw new IllegalArgumentException("tracking id is required.");

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
                rule = new TimeSeriesAggregationRule(project, aggType, getInterval(json.getField("interval")), select, filter, groupBy);
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

    public static Interval getInterval(Object interval) {
        if (interval == null)
            throw new IllegalArgumentException("interval is required for time-series.");
        if(interval instanceof String) {
            return Interval.parse((String) interval);
        }else {
            throw new IllegalArgumentException("interval parameter must be either string of number");
        }
    }

    public static FieldScript getField(Object field) {
        if (field != null) {
            if (field instanceof JsonObject) {
                String script = ((JsonObject) field).getString("script");
                if (script != null)
                    return new MVELFieldScript(script);
            } else if (field instanceof String) {
                return new SimpleFieldScript((String) field);
            }
        }
        return null;
    }

    public static Tuple<Predicate, Boolean> generatePredicate(JsonElement from) {
        Predicate to = null;
        boolean requiresUser = false;

        if (from.isArray()) {
            for (Object item : from.asArray()) {
                if (item instanceof JsonArray) {
                    JsonArray item1 = (JsonArray) item;
                    String field = item1.get(0);
                    if (field.startsWith("_user.")) {
                        requiresUser = true;
                    }
                    Predicate predicate = createPredicate(item1.get(1), field, item1.get(2));
                    to = to == null ? predicate : to.and(predicate);
                } else if (item instanceof JsonObject) {
                    Tuple<Predicate, Boolean> predicateBooleanTuple = generatePredicate((JsonElement) item);
                    Predicate predicate = predicateBooleanTuple.v1();
                    requiresUser = requiresUser || predicateBooleanTuple.v2();
                    to = to == null ? predicate : to.or(predicate);
                }
            }
        } else {
            JsonObject jsonObject = from.asObject();
            for (String fieldName : jsonObject.getFieldNames()) {
                Tuple<Predicate, Boolean> predicateBooleanTuple = generatePredicate(jsonObject.getElement(fieldName));
                Predicate predicate = predicateBooleanTuple.v1();
                requiresUser = requiresUser || predicateBooleanTuple.v2();
                switch (fieldName) {
                    case "AND":
                        to = to == null ? predicate : to.or(predicate);
                        break;
                    case "OR":
                        to = to == null ? predicate : to.or(predicate);
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
            }
        }
        return new Tuple(to, requiresUser);
    }

    private static Predicate createPredicate(String operator, String field, Object argument) {
        switch (operator) {
            case "$lte":
                return FilterPredicates.lte(field, ((Number) argument).longValue());
            case "$lt":
                return FilterPredicates.lt(field, ((Number) argument).longValue());
            case "$gt":
                return FilterPredicates.gt(field, ((Number) argument).longValue());
            case "$gte":
                return FilterPredicates.gte(field, ((Number) argument).longValue());
            case "$contains":
                return FilterPredicates.contains(field);
            case "$eq":
                return FilterPredicates.eq(field, argument);
            case "$in":
                return FilterPredicates.in(field, ((JsonArray) argument).toArray());
            case "$ne":
                return FilterPredicates.ne(field, argument);
            case "$regex":
                return FilterPredicates.regex(field, (String) argument);
            case "$starts_with":
                return FilterPredicates.starts_with(field, (String) argument);
            default:
                throw new IllegalArgumentException("undefined operator "+operator);
        }
    }

    public static FilterScript getFilter(JsonElement field) {
        if (field == null) {
            return null;
        }
        if (field.isObject()) {
            String script = ((JsonObject) field).getString("script");
            if (script != null) {
                return new MVELFilterScript(script);
            }
        }

        Tuple<Predicate, Boolean> predicateBooleanTuple = generatePredicate(field);
        return new SimpleFilterScript(predicateBooleanTuple.v1(), predicateBooleanTuple.v2());
    }
}
