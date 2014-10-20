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
import org.rakam.cache.hazelcast.hyperloglog.HLLWrapper;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.database.DatabaseAdapter;
import org.rakam.util.ConversionUtil;
import org.rakam.util.Interval;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toMap;
import static org.rakam.constant.AggregationAnalysis.SUM_X;
import static org.rakam.constant.AggregationType.AVERAGE_X;
import static org.rakam.constant.AggregationType.MINIMUM_X;
import static org.rakam.util.DateUtil.UTCTime;
import static org.rakam.util.JsonHelper.returnError;

/**
 * Created by buremba on 07/05/14.
 */
public class EventAnalyzer {
    final private DatabaseAdapter databaseAdapter;
    final private CacheAdapter cacheAdapter;
    final private DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    final private ZoneId timezone = ZoneId.of("UTC");


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
                final Interval interval;
                try {
                    interval = AnalysisRuleParser.getInterval(query.getField("interval"));
                } catch (IllegalArgumentException e) {
                    return returnError(e.getMessage());
                }

                for (AnalysisRule rule : rules) {
                    if (rule instanceof TimeSeriesAggregationRule) {
                        TimeSeriesAggregationRule r = (TimeSeriesAggregationRule) rule;
                        TimeSeriesAggregationRule tRule = new TimeSeriesAggregationRule(tracker, r.type, interval, select, filter, group_by);
                        if (rule.equals(tRule)) {
                            return fetch(tRule, query, aggAnalysis);
                        } else if (tRule.isMultipleInterval(r)) {
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

    private JsonObject combineTimeSeries(TimeSeriesAggregationRule mRule, JsonObject query, Interval interval, AggregationAnalysis aggAnalysis) {
        JsonObject json = new JsonObject();
        Long items = ConversionUtil.toLong(query.getField("items"), 10L);
        if (items == null)
            return returnError("items parameter is not numeric");

        List<Integer> keys;
        try {
            keys = getTimeFrame(query.getField("frame"), interval, query.getString("start"), query.getString("end"));
        } catch (IllegalArgumentException e) {
            return returnError(e.getMessage());
        }

        final long numberOfSubIntervals = interval.divide(interval);
        int nowKey = mRule.interval.spanCurrent().current();

        final JsonObject metadata = new JsonObject()
                .putString("start_date", Instant.ofEpochSecond(keys.get(0)).atZone(timezone).format(formatter))
                .putString("end_date", Instant.ofEpochSecond(keys.get(keys.size() - 1)).atZone(timezone).format(formatter));
        json.putObject("metadata", metadata);

        JsonObject calculatedResult = new JsonObject();
        if (mRule.groupBy == null)
            switch (aggAnalysis) {
                case COUNT:
                case COUNT_X:
                case SUM_X:
                    for (Integer key : keys) {
                        List<Integer> objects = getTimestampsBetween(key, interval, mRule.interval);

                        JsonObject result = fetchNonGroupingTimeSeries(mRule, objects, aggAnalysis);
                        long sum = 0;
                        for (String field : result.getFieldNames())
                            sum += result.getNumber(field).longValue();

                        calculatedResult.putNumber(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), sum);
                    }
                    break;
                case MINIMUM_X:
                case MAXIMUM_X:
                    for (Integer key : keys) {
                        List<Integer> objects = getTimestampsBetween(key, interval, mRule.interval);

                        JsonObject result = fetchNonGroupingTimeSeries(mRule, objects, aggAnalysis);
                        long val = mRule.type == AggregationType.MINIMUM_X ? Integer.MAX_VALUE : Integer.MIN_VALUE;
                        for (String field : result.getFieldNames())
                            if (mRule.type == AggregationType.MINIMUM_X)
                                val = Math.min(result.getNumber(field).longValue(), val);
                            else
                                val = Math.max(result.getNumber(field).longValue(), val);

                        calculatedResult.putNumber(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), val);
                    }

                    break;
                case AVERAGE_X:
                    for (Integer key : keys) {
                        long sum = 0;
                        long count = 0;
                        final Interval.StatefulSpanTime span = interval.span(key);

                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            CacheAdapter adapter = nowKey == span.current() ? cacheAdapter : databaseAdapter;
                            AverageCounter averageCounter = adapter.getAverageCounter(mRule.id() + ":" + span.current());
                            if (averageCounter != null) {
                                sum += averageCounter.getSum();
                                count += averageCounter.getCount();
                            }
                            span.next();
                        }
                        calculatedResult.putNumber(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), count > 0 ? sum / count : 0);
                    }
                    break;
                case COUNT_UNIQUE_X:
                    for (Integer key : keys) {
                        ArrayList<String> itemKeys = new ArrayList<>();
                        HLLWrapper hll = null;

                        final Interval.StatefulSpanTime span = interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            if (nowKey != span.current())
                                itemKeys.add(mRule.id() + ":" + span.current());
                            else
                                hll = cacheAdapter.createHLLFromSets(mRule.id() + ":" + span.current());
                            span.next();
                        }

                        if (itemKeys.size() > 0) {
                            HLLWrapper dbHll = databaseAdapter.createHLLFromSets(itemKeys.stream().toArray(String[]::new));
                            if (hll == null)
                                hll = dbHll;
                            else
                                hll.union(dbHll);
                        }
                        calculatedResult.putNumber(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), hll.cardinality());
                    }
                    break;
                case SELECT_UNIQUE_X:
                    for (Integer key : keys) {
                        Set<String> set = new HashSet<>();

                        final Interval.StatefulSpanTime span = interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            CacheAdapter adapter = nowKey == span.current() ? cacheAdapter : databaseAdapter;
                            Set<String> itemSet = adapter.getSet(mRule.id() + ":" + span.current());
                            if (itemSet != null) {
                                set.addAll(itemSet);
                            }
                            span.next();
                        }
                        calculatedResult.putArray(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), new JsonArray(set.stream().toArray(String[]::new)));
                    }
                    break;
            }
        else {
            switch (aggAnalysis) {
                case COUNT:
                case COUNT_X:
                case SUM_X:
                    for (Integer key : keys) {
                        List<Integer> objects = getTimestampsBetween(key, interval, mRule.interval);

                        JsonObject result = fetchGroupingTimeSeries(mRule, objects, aggAnalysis, items.intValue());
                        HashMap<String, Object> sum = new HashMap<>();
                        for (String timestamp : result.getFieldNames()) {
                            JsonObject timestampItems = result.getObject(timestamp);
                            for (String item : timestampItems.getFieldNames()) {
                                Long existingCounter = (Long) sum.getOrDefault(item, 0L);
                                sum.put(item, existingCounter + timestampItems.getLong(item));
                            }
                        }
                        calculatedResult.putObject(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), new JsonObject(sum));
                    }
                    break;
                case MINIMUM_X:
                case MAXIMUM_X:
                    for (Integer key : keys) {
                        List<Integer> objects = getTimestampsBetween(key, interval, mRule.interval);

                        JsonObject result = fetchGroupingTimeSeries(mRule, objects, aggAnalysis, items.intValue());
                        HashMap<String, Object> sum = new HashMap<>();
                        for (String timestamp : result.getFieldNames()) {
                            JsonObject timestampItems = result.getObject(timestamp);
                            for (String item : timestampItems.getFieldNames()) {
                                long value = timestampItems.getLong(item);
                                Long existingCounter = (Long) sum.get(item);
                                if (existingCounter == null || (mRule.type == MINIMUM_X ? existingCounter > value : existingCounter < value))
                                    sum.put(item, value);

                            }
                        }
                        calculatedResult.putObject(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), new JsonObject(sum));
                    }

                    break;
                case AVERAGE_X:
                    for (Integer key : keys) {

                        Map<String, AverageCounter> groups = new HashMap<>();

                        final Interval.StatefulSpanTime span = interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            CacheAdapter adapter = nowKey == span.current() ? cacheAdapter : databaseAdapter;
                            Map<String, AverageCounter> averageCounter = adapter.getGroupByAverageCounters(mRule.id() + ":" + span.current());
                            if (averageCounter != null) {
                                averageCounter.forEach((k, counter) -> groups.compute(k, (gk, v) -> {
                                    if (v == null)
                                        return counter;
                                    counter.add(counter.getSum(), counter.getCount());
                                    return counter;
                                }));
                            }
                            span.next();
                        }

                        calculatedResult.putObject(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), new JsonObject(groups.entrySet().stream()
                                .collect(toMap(Map.Entry::getKey, e -> e.getValue().getValue()))));

                    }

                    break;
                case COUNT_UNIQUE_X:
                    for (Integer key : keys) {
                        Map<String, HLLWrapper> hll = null;


                        final Interval.StatefulSpanTime span = interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            CacheAdapter adapter = nowKey != span.current() ? databaseAdapter : cacheAdapter;
                            Map<String, HLLWrapper> dbHll = adapter.estimateGroupByStrings(mRule.id() + ":" + span.next(), items.intValue());
                            if (hll == null) {
                                hll = dbHll;
                            } else {
                                for (Map.Entry<String, HLLWrapper> entry : dbHll.entrySet()) {
                                    hll.merge(entry.getKey(), entry.getValue(), (a, b) -> {
                                        a.union(b);
                                        return a;
                                    });
                                }
                            }
                            span.next();
                        }
                        final JsonObject grouped = new JsonObject();
                        hll.forEach((k, v) -> grouped.putNumber(k, v.cardinality()));
                        calculatedResult.putObject(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), grouped);
                    }
                    break;
                case SELECT_UNIQUE_X:
                    for (Integer key : keys) {
                        JsonObject map = new JsonObject();

                        final Interval.StatefulSpanTime span = interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            CacheAdapter adapter = nowKey == span.current() ? cacheAdapter : databaseAdapter;
                            Map<String, Set<String>> itemSet = adapter.getGroupByStrings(mRule.id() + ":" + span.current(), items.intValue());
                            for (Map.Entry<String, Set<String>> entry : itemSet.entrySet()) {
                                final JsonArray field = map.getArray(entry.getKey());
                                if(field!=null) {
                                    entry.getValue().forEach(field::add);
                                } else {
                                    map.putArray(entry.getKey(), new JsonArray(entry.getValue().toArray()));
                                }
                            }
                            span.next();
                        }
                        calculatedResult.putObject(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), map);
                    }
                    break;

            }
        }

        json.putObject("result", calculatedResult);
        if (!mRule.batch_status && mRule.strategy != AnalysisRuleStrategy.REAL_TIME)
            json.putString("info", "batch results are not combined yet");

        return json;

    }

    private static List<Integer> getTimestampsBetween(int startingPoint, Interval interval, Interval period) {
        LinkedList<Integer> objects = new LinkedList();

        final int until = interval.span(startingPoint).next().current();
        final Interval.StatefulSpanTime span = period.span(startingPoint);
        while(span.current() < until) {
            objects.add(span.current());
            span.next();
        }
        return objects;
    }

    private JsonObject fetch(AggregationRule rule, JsonObject query, AggregationAnalysis aggAnalysis) {
        Object result;
        Long items = ConversionUtil.toLong(query.getField("items"), 10L);
        if (items == null)
            return returnError("items parameter is not numeric");

        JsonObject json = new JsonObject();
        if (rule instanceof MetricAggregationRule) {
            if (rule.groupBy != null) {
                result = fetchGroupingMetric(rule, aggAnalysis, items.intValue());
            } else {
                result = fetchNonGroupingMetric(rule, aggAnalysis);
            }
        } else {
            if (rule instanceof TimeSeriesAggregationRule) {
                List<Integer> keys;
                try {
                    keys = getTimeFrame(query.getField("frame"), ((TimeSeriesAggregationRule) rule).interval, query.getField("start"), query.getField("end"));
                } catch (IllegalArgumentException e) {
                    return returnError(e.getMessage());
                }

                if (rule.groupBy == null)
                    result = fetchNonGroupingTimeSeries((TimeSeriesAggregationRule) rule, keys, aggAnalysis);
                else
                    result = fetchGroupingTimeSeries((TimeSeriesAggregationRule) rule, keys, aggAnalysis, items.intValue());

                final JsonObject metadata = new JsonObject()
                        .putString("start_date", Instant.ofEpochSecond(keys.get(0)).atZone(timezone).format(formatter))
                        .putString("end_date", Instant.ofEpochSecond(keys.get(keys.size() - 1)).atZone(timezone).format(formatter));
                json.putObject("metadata", metadata);
            } else {
                throw new IllegalArgumentException();
            }
        }

        json.putValue("result", result);
        if (!rule.batch_status && rule.strategy != AnalysisRuleStrategy.REAL_TIME)
            json.putString("info", "batch results are not combined yet");

        return json;
    }


    public Object fetchNonGroupingMetric(AggregationRule rule, AggregationAnalysis aggAnalysis) {
        String rule_id = rule.id();
        switch (aggAnalysis) {
            case SUM_X:
                if (rule.type == AVERAGE_X) {
                    return cacheAdapter.getAverageCounter(rule_id).getSum();
                } else {
                    return cacheAdapter.getCounter(rule_id);
                }
            case COUNT_X:
                if (rule.type == AVERAGE_X) {
                    return cacheAdapter.getAverageCounter(rule_id).getCount();
                } else {
                    return cacheAdapter.getCounter(rule_id);
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

    public JsonObject fetchGroupingMetric(AggregationRule rule, AggregationAnalysis aggAnalysis, Integer items) {
        String rule_id = rule.id();
        JsonObject json = new JsonObject();
        switch (aggAnalysis) {
            case COUNT_X:
                if (rule.type == AVERAGE_X) {
                    for (Map.Entry<String, AverageCounter> item : cacheAdapter.getGroupByAverageCounters(rule_id).entrySet()) {
                        json.putNumber(item.getKey(), item.getValue().getCount());
                    }
                    break;
                }
            case SUM_X:
                if (rule.type == AVERAGE_X) {
                    for (Map.Entry<String, AverageCounter> item : cacheAdapter.getGroupByAverageCounters(rule_id).entrySet()) {
                        json.putNumber(item.getKey(), item.getValue().getSum());
                    }
                    break;
                }
            case COUNT:
            case MAXIMUM_X:
            case MINIMUM_X:
                Map<String, Long> orderedCounters;
                orderedCounters = items == null ? cacheAdapter.getGroupByCounters(rule_id) : cacheAdapter.getGroupByCounters(rule_id, items);

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
        int now = rule.interval.spanCurrent().current();
        String rule_id = rule.id();

        switch (aggAnalysis) {
            case MAXIMUM_X:
            case MINIMUM_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                }
                break;
            case SUM_X:
                if (aggAnalysis.id != rule.type.id) {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        AverageCounter counter = adapter.getAverageCounter(rule_id + ":" + time);
                        results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), counter == null ? 0 : counter.getSum());
                    }
                } else {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                    }
                }
                break;
            case COUNT_X:
                if (aggAnalysis.id != rule.type.id) {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        AverageCounter counter = adapter.getAverageCounter(rule_id + ":" + time);
                        results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), counter == null ? 0 : counter.getCount());
                    }
                } else {
                    for (Integer time : keys) {
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                    }
                }
                break;
            case SELECT_UNIQUE_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    Set<String> set = adapter.getSet(rule_id + ":" + time);
                    results.putArray(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), set == null ? new JsonArray() : new JsonArray(set.toArray()));
                }
                break;
            case COUNT_UNIQUE_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), adapter.getSetCount(rule_id + ":" + time));
                }
                break;
            case AVERAGE_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    AverageCounter averageCounter = adapter.getAverageCounter(rule_id + ":" + time);
                    results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), averageCounter != null ? averageCounter.getValue() : 0);
                }
                break;
            case COUNT:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), Objects.firstNonNull(adapter.getCounter(rule_id + ":" + time), 0));
                }
                break;
            default:
                throw new IllegalStateException();
        }
        return results;
    }

    public JsonObject fetchGroupingTimeSeries(TimeSeriesAggregationRule rule, Collection<Integer> keys, AggregationAnalysis aggAnalysis, Integer items) {
        JsonObject results = new JsonObject();
        int now = rule.interval.spanCurrent().current();
        String rule_id = rule.id();

        switch (aggAnalysis) {
            case SELECT_UNIQUE_X:
                results = new JsonObject();

                for (Integer time : keys) {
                    JsonObject obj = new JsonObject();
                    Map<String, Set<String>> groupByStrings = (now == time ? cacheAdapter : databaseAdapter).getGroupByStrings(rule_id + ":" + time, items);
                    if (groupByStrings != null) {
                        for (Map.Entry<String, Set<String>> item : groupByStrings.entrySet()) {
                            obj.putArray(item.getKey(), new JsonArray(item.getValue().toArray()));
                        }
                    }
                    results.putObject(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), obj);
                }
                break;
            case COUNT_UNIQUE_X:
                for (Integer time : keys) {
                    JsonObject arr = new JsonObject();
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    Map<String, Long> groupByStringsCounts = adapter.getGroupByStringsCounts(rule_id + ":" + time, items);
                    if (groupByStringsCounts != null)
                        for (Map.Entry<String, Long> item : groupByStringsCounts.entrySet()) {
                            arr.putNumber(item.getKey(), item.getValue());
                        }

                    results.putObject(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), arr);
                }
                break;
            case AVERAGE_X:
                for (Integer time : keys) {
                    JsonObject arr = new JsonObject();
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    Map<String, AverageCounter> groupByCounters = adapter.getGroupByAverageCounters(rule_id + ":" + time, items);
                    if (groupByCounters != null) {
                        for (Map.Entry<String, AverageCounter> item : groupByCounters.entrySet()) {
                            arr.putNumber(item.getKey(), item.getValue().getValue());
                        }
                    }

                    results.putObject(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), arr);
                }
                break;
            case SUM_X:
            case COUNT_X:
                if (rule.type == AVERAGE_X) {
                    for (Integer time : keys) {
                        JsonObject arr = new JsonObject();
                        CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                        Map<String, AverageCounter> groupByCounters = adapter.getGroupByAverageCounters(rule_id + ":" + time, items);
                        if (groupByCounters != null) {
                            for (Map.Entry<String, AverageCounter> item : groupByCounters.entrySet()) {
                                AverageCounter value = item.getValue();
                                arr.putNumber(item.getKey(), aggAnalysis == SUM_X ? value.getSum() : value.getCount());
                            }
                        }

                        results.putObject(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), arr);
                    }
                    break;
                }
            default:
                for (Integer time : keys) {
                    JsonObject arr = new JsonObject();
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    Map<String, Long> groupByCounters = adapter.getGroupByCounters(rule_id + ":" + time, items);
                    if (groupByCounters != null) {
                        for (Map.Entry<String, Long> item : groupByCounters.entrySet()) {
                            arr.putNumber(item.getKey(), item.getValue());
                        }
                    }

                    results.putObject(Instant.ofEpochSecond(time).atZone(timezone).format(formatter), arr);
                }
                break;
        }
        return results;
    }

    public List<Integer> getTimeFrame(Object items_obj, Interval period, Object start, Object end) throws IllegalArgumentException {
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

        if (items != null && items > 5000) {
            throw new IllegalArgumentException("items must be lower than 5000");
        }

        Integer s_timestamp = null;
        Integer e_timestamp = null;
        if (start != null) {
            if (start instanceof Number) {
                s_timestamp = ((Number) start).intValue();
            } else {
                try {
                    s_timestamp = ((Long) LocalDateTime.parse(start.toString(), DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC).getEpochSecond()).intValue();
                } catch (Exception e) {
                    throw new IllegalArgumentException("couldn't parse start time.");
                }
            }
        }

        if (end != null) {
            if (start instanceof Number) {
                e_timestamp = ((Number) end).intValue();
            } else {
                try {
                    e_timestamp = ((Long) LocalDateTime.parse(end.toString(), DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC).getEpochSecond()).intValue();
                } catch (Exception e) {
                    throw new IllegalArgumentException("couldn't parse end time.");
                }
            }
            if (e_timestamp == null) {
                throw new IllegalArgumentException("couldn't parse end time.");
            }
        }

        if (s_timestamp != null && e_timestamp != null) {
            Interval.StatefulSpanTime point = period.span(s_timestamp);
            int ending_point = period.span(e_timestamp).current();
            if (point.current() > ending_point) {
                throw new IllegalArgumentException("start time must be lower than end time.");
            }

            if (point.untilTimeFrame(ending_point) > 5000) {
                throw new IllegalArgumentException("there is more than 5000 items between start and times.");
            }

            while (true) {
                int current = point.current();
                if (current > ending_point)
                    break;
                point.next();
                keys.add(current);
            }

        } else if (s_timestamp != null && items_obj != null) {
            Interval.StatefulSpanTime starting_point = period.span(s_timestamp);
            int ending_point = UTCTime();
            if (starting_point.current() > ending_point) {
                throw new IllegalArgumentException("start time must be lower than end time.");
            }

            if (starting_point.untilTimeFrame(ending_point) > 5000) {
                throw new IllegalArgumentException("there is more than 5000 items between start and times.");
            }

            for (int i = 0; i < items; i++) {
                keys.add(starting_point.current());
                starting_point = starting_point.next();
                if (starting_point.current() > ending_point)
                    break;
            }
        } else if (e_timestamp != null && items_obj != null) {
            Interval.StatefulSpanTime ending_point = period.span(e_timestamp);

            for (int i = 1; i < items; i++) {
                keys.add(ending_point.current());
                ending_point = ending_point.previous();
            }
        } else if (items_obj != null) {
            final Interval.StatefulSpanTime cursor = period.spanCurrent();
            keys.add(cursor.current());
            Interval.StatefulSpanTime interval;
            for (int i = 1; i < items; i++) {
                interval = cursor.previous();
                keys.addFirst(interval.current());
            }
        } else {
            throw new IllegalArgumentException("time frame is invalid. usage: [start, end], [start, frame], [end, frame], [frame].");
        }
        return keys;
    }
}
