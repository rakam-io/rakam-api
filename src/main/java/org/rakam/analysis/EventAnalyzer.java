package org.rakam.analysis;

import com.google.common.base.Objects;
import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.database.DatabaseAdapter;
import org.rakam.util.ConversionUtil;
import org.rakam.util.SpanTime;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.rakam.util.ConversionUtil.parseDate;
import static org.rakam.util.DateUtil.UTCTime;
import static org.rakam.util.JsonHelper.returnError;

/**
 * Created by buremba on 07/05/14.
 */
public class EventAnalyzer {
    private final DatabaseAdapter databaseAdapter;
    private final CacheAdapter cacheAdapter;

    public EventAnalyzer(CacheAdapter cacheAdapter, DatabaseAdapter databaseAdapter) {
        this.databaseAdapter = databaseAdapter;
        this.cacheAdapter = cacheAdapter;
    }

    JsonObject parseQuery(AggregationAnalysis aggAnalysis, String tracker, JsonObject query) {
        Set<AnalysisRule> rules = DistributedAnalysisRuleMap.get(tracker);

        Analysis analysis;
        try {
            analysis = Analysis.get(query.getString("analysis"));
        } catch (IllegalArgumentException e) {
            return returnError("analysis parameter is empty or doesn't exist.");
        }
        if (rules == null)
            return returnError("tracker id is not exist.");

        if (analysis == null)
            return returnError("analysis type is required.");

        if (aggAnalysis == null)
            return returnError("aggregation analysis type is required.");

        FieldScript group_by = AnalysisRuleParser.getField(query.getString("group_by"));
        FieldScript select = AnalysisRuleParser.getField(query.getString("select"));
        FilterScript filter = AnalysisRuleParser.getFilter(query.getObject("filter"));

        switch (analysis) {
            case ANALYSIS_TIMESERIES:
                Integer intervalStr = query.getInteger("interval");
                SpanTime interval;
                if (intervalStr != null)
                    interval = new SpanTime(intervalStr);
                else {
                    return returnError("interval parameter required for timeseries.");
                }

                for (AnalysisRule rule : rules) {
                    if (rule instanceof TimeSeriesAggregationRule) {
                        TimeSeriesAggregationRule r = (TimeSeriesAggregationRule) rule;
                        TimeSeriesAggregationRule tRule = new TimeSeriesAggregationRule(tracker, r.type, interval.period, select, filter, group_by);
                        if (rule.equals(tRule)) {
                            return fetch(tRule, query, aggAnalysis);
                        } else if (r.isMultipleInterval(tRule)) {
                            return combineTimeSeries(((TimeSeriesAggregationRule) rule), query, interval, aggAnalysis);
                        } else if (rule.canAnalyze(aggAnalysis)) {
                            // todo
                        }
                    }
                }
                break;
            case ANALYSIS_METRIC:
                for (AnalysisRule rule : rules) {
                    if (rule instanceof MetricAggregationRule) {
                        AggregationType rType = ((MetricAggregationRule) rule).type;
                        MetricAggregationRule mRule = new MetricAggregationRule(tracker, rType, select, filter, group_by);
                        if (rule.equals(mRule) && rule.canAnalyze(aggAnalysis)) {
                            return fetch((AggregationRule) rule, query, aggAnalysis);
                        }
                    }
                }
                break;
            default:
                throw new IllegalArgumentException();

        }
        return returnError("aggregation rule couldn't found. you have to create rule in order the perform this query.");
    }

    public JsonObject analyze(AggregationAnalysis aggAnalysis, String tracker, JsonObject query) {
        Set<AnalysisRule> rules = DistributedAnalysisRuleMap.get(tracker);

        if (!query.containsField("_id")) {
            return parseQuery(aggAnalysis, tracker, query);
        } else {
            Optional<AnalysisRule> rule = rules.stream().filter(x -> x.id().equals(query.getString("_id"))).findAny();

            if (rule.isPresent()) {
                if (rule.get() instanceof AggregationRule) {
                    AggregationRule mRule = (AggregationRule) rule.get();
                    if (!mRule.canAnalyze(aggAnalysis))
                        return returnError("cannot analyze");
                    return fetch(mRule, query, aggAnalysis);
                }
            }

            return returnError("aggregation rule couldn't found. you have to create rule in order the perform this query.");

        }

    }

    private JsonObject combineTimeSeries(TimeSeriesAggregationRule mRule, JsonObject query, SpanTime interval, AggregationAnalysis aggAnalysis) {
        JsonObject calculatedResult = new JsonObject();
        Long items = ConversionUtil.toLong(query.getField("items"), 10L);
        if (items == null)
            return returnError("items parameter is not numeric");

        List<Integer> keys;
        try {
            keys = getTimeFrame(query.getField("frame"), interval.period, query.getString("start"), query.getString("end"));
        } catch (IllegalArgumentException e) {
            return returnError(e.getMessage());
        }

        if (mRule.groupBy == null)
            switch (mRule.type) {
                case COUNT:
                case COUNT_X:
                case SUM_X:
                    for (Integer key : keys) {
                        LinkedList<Integer> objects = new LinkedList<>();
                        for (int i = key; i < key + interval.period; i += mRule.interval)
                            objects.add(i);

                        JsonObject result = fetchNonGroupingTimeSeries(mRule, objects, aggAnalysis);
                        long sum = 0;
                        for (String field : result.getFieldNames())
                            sum += result.getNumber(field).longValue();

                        calculatedResult.putNumber(key + "000", sum);
                    }
                    break;
                case MINIMUM_X:
                case MAXIMUM_X:
                    for (Integer key : keys) {
                        LinkedList<Integer> objects = new LinkedList<>();
                        for (int i = key; i < key + interval.period; i += mRule.interval)
                            objects.add(i);

                        JsonObject result = fetchNonGroupingTimeSeries(mRule, objects, aggAnalysis);
                        long val = mRule.type == AggregationType.MINIMUM_X ? Integer.MAX_VALUE : Integer.MIN_VALUE;
                        for (String field : result.getFieldNames())
                            if (mRule.type == AggregationType.MINIMUM_X)
                                val = Math.min(result.getNumber(field).longValue(), val);
                            else
                                val = Math.max(result.getNumber(field).longValue(), val);

                        calculatedResult.putNumber(key + "000", val);
                    }

                    break;
                case AVERAGE_X:
                    // todo
                    break;
                case UNIQUE_X:
                    // TODO
                    break;

            }
        else {
            switch (mRule.type) {
                case COUNT:
                case COUNT_X:
                case SUM_X:
                    for (Integer key : keys) {
                        LinkedList<Integer> objects = new LinkedList();
                        for (int i = key; i < key + interval.period; i += mRule.interval)
                            objects.add(i);

                        JsonObject result = fetchGroupingTimeSeries(mRule, objects, aggAnalysis, items.intValue());
                        HashMap<String, Object> sum = new HashMap<>();
                        for (String timestamp : result.getFieldNames()) {
                            JsonObject timestampItems = result.getObject(timestamp);
                            for (String item : timestampItems.getFieldNames()) {
                                Long existingCounter = (Long) sum.getOrDefault(item, 0L);
                                Long newCounter = timestampItems.getLong(item);
                                sum.put(item, existingCounter + newCounter);
                            }
                        }
                        calculatedResult.putObject(key + "000", new JsonObject(sum));

                    }
                    break;
                case MINIMUM_X:
                case MAXIMUM_X:
                    for (Integer key : keys) {
                        LinkedList<Integer> objects = new LinkedList<>();
                        for (int i = key; i < key + interval.period; i += mRule.interval)
                            objects.add(i);

                        JsonObject result = fetchNonGroupingTimeSeries(mRule, objects, aggAnalysis);
                        long val = mRule.type == AggregationType.MINIMUM_X ? Integer.MAX_VALUE : Integer.MIN_VALUE;
                        for (String field : result.getFieldNames())
                            if (mRule.type == AggregationType.MINIMUM_X)
                                val = Math.min(result.getNumber(field).longValue(), val);
                            else
                                val = Math.max(result.getNumber(field).longValue(), val);

                        calculatedResult.putNumber(key + "000", val);
                    }

                    break;
                case AVERAGE_X:
                    // TODO
                    break;
                case UNIQUE_X:
                    // TODO
                    break;

            }
        }

        JsonObject json = new JsonObject().putObject("result", calculatedResult);
        if (!mRule.batch_status && mRule.strategy != AnalysisRuleStrategy.REAL_TIME)
            json.putString("info", "batch results are not combined yet");

        return json;

    }

    private JsonObject fetch(AggregationRule rule, JsonObject query, AggregationAnalysis aggAnalysis) {
        Object result;
        Long items = ConversionUtil.toLong(query.getField("items"), 10L);
        if (items == null)
            return returnError("items parameter is not numeric");


        if (rule instanceof MetricAggregationRule) {
            if (rule.groupBy != null) {
                result = fetchGroupingMetric(rule, aggAnalysis, items.intValue());
            } else {
                result = fetchNonGroupingMetric(rule, aggAnalysis);
            }
        } else {
            if (rule instanceof TimeSeriesAggregationRule) {
                Collection<Integer> keys;
                try {
                    keys = getTimeFrame(query.getField("frame"), ((TimeSeriesAggregationRule) rule).interval, query.getField("start"), query.getField("end"));
                } catch (IllegalArgumentException e) {
                    return returnError(e.getMessage());
                }

                if (rule.groupBy == null)
                    result = fetchNonGroupingTimeSeries((TimeSeriesAggregationRule) rule, keys, aggAnalysis);
                else
                    result = fetchGroupingTimeSeries((TimeSeriesAggregationRule) rule, keys, aggAnalysis, items.intValue());
            } else {
                throw new IllegalArgumentException();
            }
        }

        JsonObject json = new JsonObject().putValue("result", result);
        if (!rule.batch_status && rule.strategy != AnalysisRuleStrategy.REAL_TIME)
            json.putString("info", "batch results are not combined yet");

        return json;
    }


    public Object fetchNonGroupingMetric(AggregationRule rule, AggregationAnalysis aggAnalysis) {
        String rule_id = rule.id();
        switch (aggAnalysis) {
            case SUM_X:
                if (aggAnalysis.id != rule.type.id) {
                    return cacheAdapter.getAverageCounter(rule_id).getSum();
                }
            case COUNT_X:
                if (aggAnalysis.id != rule.type.id) {
                    return cacheAdapter.getAverageCounter(rule_id).getCount();
                }
            case COUNT:
            case MAXIMUM_X:
            case MINIMUM_X:
                return cacheAdapter.getCounter(rule_id);
            case AVERAGE_X:
                return cacheAdapter.getAverageCounter(rule_id).getValue();
            case COUNT_UNIQUE_X:
                return cacheAdapter.getSetCount(rule_id);
            case SELECT_UNIQUE_X:
                return new JsonArray(cacheAdapter.getSet(rule_id).toArray());
        }
        return null;
    }

    public Object fetchGroupingMetric(AggregationRule rule, AggregationAnalysis aggAnalysis, Integer items) {
        String rule_id = rule.id();
        JsonObject json = new JsonObject();
        switch (aggAnalysis) {
            case COUNT:
            case COUNT_X:
            case SUM_X:
            case MAXIMUM_X:
            case MINIMUM_X:
                Map<String, Long> orderedCounters;
                if (items == null) {
                    orderedCounters = cacheAdapter.getGroupByCounters(rule_id);
                } else {
                    orderedCounters = cacheAdapter.getGroupByCounters(rule_id, items);
                }

                for (Map.Entry<String, Long> counter : orderedCounters.entrySet()) {
                    json.putNumber(counter.getKey(), counter.getValue());
                }

                break;
            case AVERAGE_X:
                for (Map.Entry<String, AverageCounter> item : cacheAdapter.getGroupByAverageCounters(rule_id).entrySet()) {
                    json.putNumber(item.getKey(), item.getValue().getValue());
                }

                break;
            case COUNT_UNIQUE_X:
                for (Map.Entry<String, Long> item : cacheAdapter.getGroupByStringsCounts(rule_id, items).entrySet()) {
                    json.putNumber(item.getKey(), item.getValue());
                }
                break;
            case SELECT_UNIQUE_X:
                for (Map.Entry<String, Set<String>> item : cacheAdapter.getGroupByStrings(rule_id, items).entrySet()) {
                    json.putArray(item.getKey(), new JsonArray(item.getValue().toArray()));
                }
                break;
        }
        return json;
    }

    public JsonObject fetchNonGroupingTimeSeries(TimeSeriesAggregationRule rule, Collection<Integer> keys, AggregationAnalysis aggAnalysis) {
        JsonObject results = new JsonObject();
        int now = new SpanTime(rule.interval).spanCurrent().current();
        String rule_id = rule.id();

        switch (aggAnalysis) {
            case MAXIMUM_X:
            case MINIMUM_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(time + "000", Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                }
                break;
            case SUM_X:
                if (aggAnalysis.id != rule.type.id) {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        AverageCounter counter = adapter.getAverageCounter(rule_id+":"+time);
                        results.putNumber(time + "000", counter==null ? 0 : counter.getSum());
                    }
                }else {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        results.putNumber(time + "000", Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                    }
                }
                break;
            case COUNT_X:
                if (aggAnalysis.id != rule.type.id) {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        AverageCounter counter = adapter.getAverageCounter(rule_id+":"+time);
                        results.putNumber(time + "000", counter==null ? 0 : counter.getCount());
                    }
                }else {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        results.putNumber(time + "000", Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                    }
                }
                break;
            case SELECT_UNIQUE_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    Set<String> set = adapter.getSet(rule_id + ":" + time);
                    results.putArray(time + "000", set==null ? new JsonArray() : new JsonArray(set.toArray()));
                }
                break;
            case COUNT_UNIQUE_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(time + "000", adapter.getSetCount(rule_id + ":" + time));
                }
                break;
            case AVERAGE_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    AverageCounter averageCounter = adapter.getAverageCounter(rule_id + ":" + time);
                    results.putNumber(time + "000", averageCounter!=null ? averageCounter.getValue() : 0);
                }
                break;
            case COUNT:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(time + "000", Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                }
                break;
            default:
                throw new IllegalStateException();
        }
        return results;
    }

    public JsonObject fetchGroupingTimeSeries(TimeSeriesAggregationRule rule, Collection<Integer> keys, AggregationAnalysis aggAnalysis, Integer items) {
        JsonObject results = new JsonObject();
        int now = new SpanTime(rule.interval).spanCurrent().current();
        String rule_id = rule.id();

        switch (aggAnalysis) {
            case SELECT_UNIQUE_X:
                results = new JsonObject();

                for (Integer time : keys) {
                    JsonObject obj = new JsonObject();
                    for (Map.Entry<String, Set<String>> item : (now == time ? cacheAdapter : databaseAdapter).getGroupByStrings(rule_id + ":" + time, items).entrySet()) {
                        obj.putArray(item.getKey(), new JsonArray(item.getValue().toArray()));
                    }
                    results.putObject(time + "000", obj);
                }
                break;
            case COUNT_UNIQUE_X:
                for (Integer time : keys) {
                    JsonObject arr = new JsonObject();
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    for (Map.Entry<String, Long> item : adapter.getGroupByStringsCounts(rule_id + ":" + time, items).entrySet()) {
                        arr.putNumber(item.getKey(), item.getValue());
                    }

                    results.putObject(time + "000", arr);
                }
                break;
            default:
                for (Integer time : keys) {
                    JsonObject arr = new JsonObject();
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    for (Map.Entry<String, Long> item : adapter.getGroupByCounters(rule_id + ":" + time, items).entrySet()) {
                        arr.putNumber(item.getKey(), item.getValue());
                    }

                    results.putObject(time + "000", arr);
                }
                break;
        }
        return results;
    }

    public static List<Integer> getTimeFrame(Object items_obj, int period, Object start, Object end) throws IllegalArgumentException {
        Integer items;
        if (items_obj != null) {
            if (items_obj instanceof Number)
                items = (Integer) items_obj;
            else
                try {
                    items = Integer.parseInt((String) items_obj);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("frame parameter is required.");
                }
        } else {
            items = null;
        }

        LinkedList<Integer> keys = new LinkedList();

        if (items != null && items > 100) {
            throw new IllegalArgumentException("items must be lower than 100");
        }

        Integer s_timestamp = null;
        Integer e_timestamp = null;
        if (start != null) {
            if(start instanceof Number) {
                s_timestamp = ((Number) start).intValue();
            }else {
                s_timestamp = parseDate(start.toString());
                if (s_timestamp == null) {
                    throw new IllegalArgumentException("couldn't parse start time.");
                }
            }
        }

        if (end != null) {
            if(start instanceof Number) {
                e_timestamp = ((Number) end).intValue();
            }else {
                e_timestamp = parseDate(end.toString());
                if (s_timestamp == null) {
                    throw new IllegalArgumentException("couldn't parse end time.");
                }
            }
            if (e_timestamp == null) {
                throw new IllegalArgumentException("couldn't parse end time.");
            }
        }

        SpanTime interval = new SpanTime(period);

        if (s_timestamp != null && e_timestamp != null) {
            SpanTime point = interval.span(s_timestamp);
            int ending_point = interval.copy().span(e_timestamp).current();
            if (point.current() > ending_point) {
                throw new IllegalArgumentException("start time must be lower than end time.");
            }

            if (point.untilTimeFrame(ending_point) > 100) {
                throw new IllegalArgumentException("there is more than 100 items between start and times.");
            }

            while(true) {
                int current = point.current();
                if (current > ending_point)
                    break;
                point.next();
                keys.add(current);
            }

        } else if (s_timestamp != null && items_obj != null) {
            SpanTime starting_point = interval.span(s_timestamp);
            int ending_point = UTCTime();
            if (starting_point.current() > ending_point) {
                throw new IllegalArgumentException("start time must be lower than end time.");
            }

            if (starting_point.untilTimeFrame(ending_point) > 100) {
                throw new IllegalArgumentException("there is more than 100 items between start and times.");
            }

            for (int i = 0; i < items; i++) {
                keys.add(starting_point.current());
                starting_point = starting_point.next();
                if (starting_point.current() > ending_point)
                    break;
            }
        } else if (e_timestamp != null && items_obj != null) {
            SpanTime ending_point = interval.span(e_timestamp);

            for (int i = 1; i < items; i++) {
                keys.add(ending_point.current());
                ending_point = ending_point.previous();
            }
        } else if (items_obj != null) {
            keys.add(interval.spanCurrent().current());
            for (int i = 1; i < items; i++) {
                interval = interval.previous();
                keys.addFirst(interval.current());
            }
        } else {
            throw new IllegalArgumentException("time frame is invalid. usage: [start, end], [start, frame], [end, frame], [frame].");
        }
        return keys;
    }
}
