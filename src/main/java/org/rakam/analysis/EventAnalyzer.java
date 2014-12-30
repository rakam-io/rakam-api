package org.rakam.analysis;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;
import com.google.inject.Injector;
import org.rakam.analysis.query.FieldScript;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.stream.MetricStreamAdapter;
import org.rakam.stream.TimeSeriesStreamAdapter;
import org.rakam.stream.hazelcast.hyperloglog.HLLWrapper;
import org.rakam.stream.AverageCounter;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.util.ConversionUtil;
import org.rakam.util.Interval;
import org.rakam.util.Tuple;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

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
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.rakam.constant.AggregationAnalysis.SUM_X;
import static org.rakam.constant.AggregationType.AVERAGE_X;
import static org.rakam.constant.AggregationType.MINIMUM_X;
import static org.rakam.util.JsonHelper.returnError;
import static org.rakam.util.TimeUtil.UTCTime;

/**
 * Created by buremba on 07/05/14.
 */
public class EventAnalyzer {
    final private MetricStreamAdapter metricStorageAdapter;
    final private TimeSeriesStreamAdapter memoryTimeSeriesStorageAdapter;
    final private TimeSeriesStreamAdapter diskTimeSeriesStorageAdapter;


    final private DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    final private ZoneId timezone = ZoneId.of("UTC");
    private final AnalysisRuleMap analysisRuleMap;

    public EventAnalyzer(Injector injector, AnalysisRuleMap analysisRuleMap) {
        this.metricStorageAdapter = injector.getInstance(MetricStreamAdapter.class);
        this.memoryTimeSeriesStorageAdapter = injector.getInstance(TimeSeriesStreamAdapter.class);
        this.diskTimeSeriesStorageAdapter = injector.getInstance(TimeSeriesStreamAdapter.class);
        this.analysisRuleMap = analysisRuleMap;
    }

    JsonObject parseQuery(AggregationAnalysis aggAnalysis, String tracker, JsonObject query) {
        Set<AnalysisRule> rules = analysisRuleMap.get(tracker);

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

    public JsonObject analyze(JsonObject query) {
        final AggregationAnalysis aggAnalysis;
        try {
            aggAnalysis = AggregationAnalysis.get(query.getString("analysis_type"));
        } catch (IllegalArgumentException | NullPointerException e) {
            return returnError("analysis_type parameter is empty or doesn't exist.");
        }

        String tracker = query.getString("tracker");

        if (tracker == null) {
            return returnError("tracker parameter is required.");
        }

        Set<AnalysisRule> rules = analysisRuleMap.get(tracker);

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
        Integer items = query.getInteger("items", 10);
        if (items == null)
            return returnError("items parameter is not numeric");

        List<Integer> keys;
        try {
            keys = getTimeFrame(query.getField("frame"), interval, query.getString("start"), query.getString("end"));
        } catch (IllegalArgumentException e) {
            return returnError(e.getMessage());
        }

        final long numberOfSubIntervals = interval.divide(mRule.interval);
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

                        JsonObject result = fetchNonGroupingTimeSeries(mRule, objects, aggAnalysis, items.intValue());
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

                        JsonObject result = fetchNonGroupingTimeSeries(mRule, objects, aggAnalysis, items.intValue());
                        Long val = null;
                        for (String field : result.getFieldNames()) {
                            Number number = result.getNumber(field);
                            if (number != null)
                                if (val == null)
                                    val = number.longValue();
                                else if (mRule.type == AggregationType.MINIMUM_X)
                                    val = Math.min(number.longValue(), val);
                                else
                                    val = Math.max(number.longValue(), val);
                        }

                        calculatedResult.putNumber(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), val);
                    }

                    break;
                case AVERAGE_X:
                    for (Integer key : keys) {
                        long sum = 0;
                        long count = 0;
                        final Interval.StatefulSpanTime span = mRule.interval.span(key);

                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            // todo: consider that it may not be available on disk immediately. fallback to memory in this case
                            TimeSeriesStreamAdapter adapter = nowKey == span.current() ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                            AverageCounter averageCounter = adapter.getTimeSeriesAverageCounter(mRule.id(), span.current());
                            if (averageCounter != null) {
                                sum += averageCounter.getSum();
                                count += averageCounter.getCount();
                            }
                            span.next();
                        }
                        calculatedResult.putNumber(formatTime(key), count > 0 ? sum / count : 0);
                    }
                    break;
                case COUNT_UNIQUE_X:
                    for (Integer key : keys) {
                        ArrayList<Integer> itemKeys = new ArrayList<>();
                        HLLWrapper hll = null;

                        final Interval.StatefulSpanTime span = mRule.interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            if (nowKey != span.current())
                                itemKeys.add(span.current());
                            else
                                hll = memoryTimeSeriesStorageAdapter.createHLLFromTimeSpans(mRule.id(), span.current());
                            span.next();
                        }

                        if (itemKeys.size() > 0) {
                            HLLWrapper dbHll = diskTimeSeriesStorageAdapter.createHLLFromTimeSpans(mRule.id(), Ints.toArray(itemKeys));
                            if (hll == null)
                                hll = dbHll;
                            else
                                hll.union(dbHll);
                        }
                        calculatedResult.putNumber(formatTime(key), hll.cardinality());
                    }
                    break;
                case SELECT_UNIQUE_X:
                    for (Integer key : keys) {
                        Set<String> set = new HashSet<>();

                        final Interval.StatefulSpanTime span = mRule.interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            TimeSeriesStreamAdapter adapter = nowKey == span.current() ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                            String[] itemSet = adapter.getTimeSeriesStrings(mRule.id(), span.current(), items);
                            if (itemSet != null) {
                                for (String s : itemSet) {
                                    set.add(s);
                                }
                            }
                            span.next();
                        }
                        calculatedResult.putArray(formatTime(key), new JsonArray(set.stream().toArray(String[]::new)));
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

                        final Interval.StatefulSpanTime span = mRule.interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            TimeSeriesStreamAdapter adapter = nowKey == span.current() ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                            Map<String, AverageCounter> averageCounter = adapter.getTimeSeriesGroupByAverageCounters(mRule.id(), span.current(), items);
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

                        Map<String, Object> collect = groups.entrySet().stream()
                                .collect(toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
                        calculatedResult.putObject(Instant.ofEpochSecond(key).atZone(timezone).format(formatter), new JsonObject(collect));

                    }

                    break;
                case COUNT_UNIQUE_X:
                    for (Integer key : keys) {
                        Map<String, HLLWrapper> hll = new HashMap<>();


                        final Interval.StatefulSpanTime span = mRule.interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            TimeSeriesStreamAdapter adapter = nowKey == span.current() ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                            Stream<Tuple<String, HLLWrapper>> dbHll = adapter.estimateTimeSeriesGroupByStrings(mRule.id(), span.current(), items);

                            dbHll.forEach(x -> {
                                hll.merge(x.v1(), x.v2(), (a, b) -> {
                                    a.union(b);
                                    return a;
                                });
                            });

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

                        final Interval.StatefulSpanTime span = mRule.interval.span(key);
                        for (long i = 0; i < numberOfSubIntervals; i++) {
                            TimeSeriesStreamAdapter adapter = nowKey == span.current() ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                            Map<String, Set<String>> itemSet = adapter.getTimeSeriesGroupByStrings(mRule.id(), span.current(), items.intValue());
                            for (Map.Entry<String, Set<String>> entry : itemSet.entrySet()) {
                                final JsonArray field = map.getArray(entry.getKey());
                                if (field != null) {
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
        while (span.current() < until) {
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
                result = fetchNonGroupingMetric(rule, aggAnalysis, items.intValue());
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
                    result = fetchNonGroupingTimeSeries((TimeSeriesAggregationRule) rule, keys, aggAnalysis, items.intValue());
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


    public Object fetchNonGroupingMetric(AggregationRule rule, AggregationAnalysis aggAnalysis, int items) {
        String rule_id = rule.id();
        switch (aggAnalysis) {
            case SUM_X:
                if (rule.type == AVERAGE_X) {
                    return metricStorageAdapter.getMetricAverageCounter(rule_id).getSum();
                } else {
                    return metricStorageAdapter.getMetricCounter(rule_id);
                }
            case COUNT_X:
                if (rule.type == AVERAGE_X) {
                    return metricStorageAdapter.getMetricAverageCounter(rule_id).getCount();
                } else {
                    return metricStorageAdapter.getMetricCounter(rule_id);
                }
            case COUNT:
            case MAXIMUM_X:
            case MINIMUM_X:
                return metricStorageAdapter.getMetricCounter(rule_id);
            case AVERAGE_X:
                return metricStorageAdapter.getMetricAverageCounter(rule_id).getValue();
            case COUNT_UNIQUE_X:
                return metricStorageAdapter.getMetricStringCount(rule_id);
            case SELECT_UNIQUE_X:
                return new JsonArray(metricStorageAdapter.getMetricStrings(rule_id, items));
        }
        return null;
    }

    public JsonObject fetchGroupingMetric(AggregationRule rule, AggregationAnalysis aggAnalysis, int items) {
        String rule_id = rule.id();
        JsonObject json = new JsonObject();
        switch (aggAnalysis) {
            case COUNT_X:
                if (rule.type == AVERAGE_X) {
                    for (Map.Entry<String, AverageCounter> item : metricStorageAdapter.getMetricGroupByAverageCounters(rule_id, items).entrySet()) {
                        json.putNumber(item.getKey(), item.getValue().getCount());
                    }
                    break;
                }
            case SUM_X:
                if (rule.type == AVERAGE_X) {
                    for (Map.Entry<String, AverageCounter> item : metricStorageAdapter.getMetricGroupByAverageCounters(rule_id, items).entrySet()) {
                        json.putNumber(item.getKey(), item.getValue().getSum());
                    }
                    break;
                }
            case COUNT:
            case MAXIMUM_X:
            case MINIMUM_X:
                Map<String, Long> orderedCounters;
                orderedCounters = metricStorageAdapter.getMetricGroupByCounters(rule_id, items);

                for (Map.Entry<String, Long> counter : orderedCounters.entrySet()) {
                    json.putNumber(counter.getKey(), counter.getValue());
                }
                break;
            case AVERAGE_X:
                for (Map.Entry<String, AverageCounter> item : metricStorageAdapter.getMetricGroupByAverageCounters(rule_id, items).entrySet()) {
                    json.putNumber(item.getKey(), item.getValue().getValue());
                }
                break;
            case COUNT_UNIQUE_X:
                for (Map.Entry<String, Long> item : metricStorageAdapter.getMetricGroupByStringsCounts(rule_id, items).entrySet()) {
                    json.putNumber(item.getKey(), item.getValue());
                }
                break;
            case SELECT_UNIQUE_X:
                for (Map.Entry<String, Set<String>> item : metricStorageAdapter.getMetricGroupByStrings(rule_id, items).entrySet()) {
                    json.putArray(item.getKey(), new JsonArray(item.getValue().toArray()));
                }
                break;
        }
        return json;
    }

    public JsonObject fetchNonGroupingTimeSeries(TimeSeriesAggregationRule rule, Collection<Integer> keys, AggregationAnalysis aggAnalysis, int items) {
        JsonObject results = new JsonObject();
        int now = rule.interval.spanCurrent().current();
        String rule_id = rule.id();

        switch (aggAnalysis) {
            case MAXIMUM_X:
            case MINIMUM_X:
                for (Integer time : keys) {
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    results.putNumber(formatTime(time), adapter.getTimeSeriesCounter(rule_id, time));
                }
                break;
            case SUM_X:
                if (aggAnalysis.id != rule.type.id) {
                    for (Integer time : keys) {
                        TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                        AverageCounter counter = adapter.getTimeSeriesAverageCounter(rule_id, time);
                        results.putNumber(formatTime(time), counter == null ? 0 : counter.getSum());
                    }
                } else {
                    for (Integer time : keys) {
                        TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                        results.putNumber(formatTime(time), Objects.firstNonNull(adapter.getTimeSeriesCounter(rule_id, time), 0));
                    }
                }
                break;
            case COUNT_X:
                if (aggAnalysis.id != rule.type.id) {
                    for (Integer time : keys) {
                        TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                        AverageCounter counter = adapter.getTimeSeriesAverageCounter(rule_id, time);
                        results.putNumber(formatTime(time), counter == null ? 0 : counter.getCount());
                    }
                } else {
                    for (Integer time : keys) {
                        TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                        results.putNumber(formatTime(time), Objects.firstNonNull(adapter.getTimeSeriesCounter(rule_id, time), 0));
                    }
                }
                break;
            case SELECT_UNIQUE_X:
                for (Integer time : keys) {
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    String[] set = adapter.getTimeSeriesStrings(rule_id, time, items);
                    results.putArray(formatTime(time), set == null ? new JsonArray() : new JsonArray(set));
                }
                break;
            case COUNT_UNIQUE_X:
                for (Integer time : keys) {
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    results.putNumber(formatTime(time), Objects.firstNonNull(adapter.getTimeSeriesStringCount(rule_id, time), 0));
                }
                break;
            case AVERAGE_X:
                for (Integer time : keys) {
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    AverageCounter averageCounter = adapter.getTimeSeriesAverageCounter(rule_id, time);
                    results.putNumber(formatTime(time), averageCounter != null ? averageCounter.getValue() : 0);
                }
                break;
            case COUNT:
                for (Integer time : keys) {
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    results.putNumber(formatTime(time), Objects.firstNonNull(adapter.getTimeSeriesCounter(rule_id, time), 0));
                }
                break;
            default:
                throw new IllegalStateException();
        }
        return results;
    }

    private String formatTime(int time) {
        return Instant.ofEpochSecond(time).atZone(timezone).format(formatter);
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
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    Map<String, Set<String>> groupByStrings = adapter.getTimeSeriesGroupByStrings(rule_id, time, items);
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
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    Map<String, Long> groupByStringsCounts = adapter.getTimeSeriesGroupByStringsCounts(rule_id, time, items);
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
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    Map<String, AverageCounter> groupByCounters = adapter.getTimeSeriesGroupByAverageCounters(rule_id, time, items);
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
                        TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                        Map<String, AverageCounter> groupByCounters = adapter.getTimeSeriesGroupByAverageCounters(rule_id, time, items);
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
                    TimeSeriesStreamAdapter adapter = now == time ? memoryTimeSeriesStorageAdapter : diskTimeSeriesStorageAdapter;
                    Map<String, Long> groupByCounters = adapter.getTimeSeriesGroupByCounters(rule_id, time, items);
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
        } else {
            items = items != null ? items : 20;
            final Interval.StatefulSpanTime cursor = period.spanCurrent();
            keys.add(cursor.current());
            Interval.StatefulSpanTime interval;
            for (int i = 1; i < items; i++) {
                interval = cursor.previous();
                keys.addFirst(interval.current());
            }
        }
//        throw new IllegalArgumentException("time frame is invalid. usage: [start, end], [start, frame], [end, frame], [frame].");

        return keys;
    }
}
