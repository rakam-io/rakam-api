package org.rakam.analysis;

import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.database.DatabaseAdapter;
import org.rakam.util.SpanTime;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.*;

import static org.rakam.util.ConversionUtil.parseDate;
import static org.rakam.util.ConversionUtil.toInt;
import static org.rakam.util.JsonHelper.returnError;

/**
 * Created by buremba on 07/05/14.
 */
public class AnalysisRequestHandler implements Handler<Message<JsonObject>> {
    private static final DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);
    private static final CacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);

    @Override
    public void handle(Message<JsonObject> event) {
        JsonObject query = event.body();
        String tracker = query.getString("tracker");

        final AggregationAnalysis aggAnalysis;
        try {
            aggAnalysis = AggregationAnalysis.get(query.getString("aggregation"));
        } catch (IllegalArgumentException|NullPointerException e) {
            event.reply(returnError("aggregation parameter is empty or doesn't exist."));
            return;
        }

        if (tracker == null) {
            event.reply(returnError("tracker parameter is required."));
            return;
        }

        AnalysisRuleList rules = DistributedAnalysisRuleMap.get(tracker);
        if(!query.containsField("_id")) {
            Analysis analysis;
            try {
                analysis = Analysis.get(query.getString("analysis"));
            } catch (IllegalArgumentException e) {
                event.reply(returnError("analysis parameter is empty or doesn't exist."));
                return;
            }
            if (rules == null) {
                event.reply(returnError("tracker id is not exist."));
                return;
            }
            if (analysis == null) {
                event.reply(returnError("analysis type is required."));
                return;
            }
            if (aggAnalysis == null) {
                event.reply(returnError("aggregation analysis type is required."));
                return;
            }
            FieldScript group_by = AnalysisRuleParser.getField(query.getString("group_by"));
            FieldScript select = AnalysisRuleParser.getField(query.getString("select"));
            FilterScript filter = AnalysisRuleParser.getFilter(query.getObject("filter"));


            if (analysis == Analysis.ANALYSIS_TIMESERIES) {

                String intervalStr = query.getString("interval");
                SpanTime interval;
                if (intervalStr != null)
                    try {
                        interval = SpanTime.fromPeriod(intervalStr);
                    } catch (IllegalArgumentException e) {
                        event.reply(returnError(e.getMessage()));
                        return;
                    }
                else {
                    event.reply(returnError("interval parameter required for timeseries."));
                    return;
                }

                for (AnalysisRule rule : rules) {
                    if (rule instanceof TimeSeriesAggregationRule) {
                        TimeSeriesAggregationRule tRule = new TimeSeriesAggregationRule(tracker, aggAnalysis.getAggregationType(), interval, select, filter, group_by);
                        if (rule.equals(tRule)) {
                            event.reply(fetch(tRule, query, aggAnalysis));
                            return;
                        }else
                        if (((TimeSeriesAggregationRule) rule).isMultipleInterval(tRule)) {
                            event.reply(combineTimeSeries(((TimeSeriesAggregationRule) rule), query, interval, aggAnalysis));
                            return;
                        }
                    }
                }
            } else if (analysis == Analysis.ANALYSIS_METRIC) {
                for (AnalysisRule rule : rules) {
                    if (rule instanceof MetricAggregationRule) {
                        MetricAggregationRule mRule = new MetricAggregationRule(tracker, aggAnalysis.getAggregationType(), select, filter, group_by);
                        if (rule.equals(mRule)) {
                            event.reply(fetch(mRule, query, aggAnalysis));
                            return;
                        }
                    }
                }
            }
        }else {
            Optional<AnalysisRule> rule = rules.stream().filter(x -> x.id.equals(query.getString("_id"))).findAny();
            rule.ifPresent(mRule -> {
                    if(mRule instanceof AggregationRule) {
                        if(!((AggregationRule) mRule).canAnalyze(aggAnalysis))
                            returnError("cannot analyze");
                        else
                            if (mRule instanceof MetricAggregationRule)
                                event.reply(fetch((MetricAggregationRule) mRule, query, aggAnalysis));
                            else if (rule.get() instanceof TimeSeriesAggregationRule)
                                event.reply(fetch((TimeSeriesAggregationRule) mRule, query, aggAnalysis));
                    }
            });
            if(!rule.isPresent())
                returnError("aggregation rule couldn't found.");


        }

        event.reply(returnError("aggregation rule couldn't found. you have to create rule in order the perform this query."));
    }

    private JsonObject combineTimeSeries(TimeSeriesAggregationRule mRule, JsonObject query, SpanTime interval, AggregationAnalysis aggAnalysis) {
        JsonObject calculatedResult = new JsonObject();
        Integer items;
        try {
            items = toInt(query.getField("items"), 10);
        } catch (IllegalArgumentException e) {
            return returnError("items parameter is not numeric");
        }

        List<Integer> keys;
        try {
            keys = getTimeFrame(query.getField("frame"), interval, query.getString("start"), query.getString("end"));
        } catch (IllegalArgumentException e) {
            return returnError(e.getMessage());
        }

        if (mRule.groupBy == null)
            switch (mRule.type) {
                case COUNT:
                case COUNT_X:
                case SUM_X:
                    for(Integer key : keys) {
                        JsonObject result = new JsonObject();
                        LinkedList<Integer> objects = new LinkedList<>();
                        for(int i=key; i<key+interval.period; i+=mRule.interval.period)
                            objects.add(i);

                        fetchNonGroupingTimeSeries(result, mRule, objects, aggAnalysis);
                        long sum = 0;
                        for(String field : result.getFieldNames())
                            sum += result.getNumber(field).longValue();

                        calculatedResult.putNumber(key+"000", sum);
                    }
                    break;
                case MINIMUM_X:
                case MAXIMUM_X:
                    for(Integer key : keys) {
                        JsonObject result = new JsonObject();
                        LinkedList<Integer> objects = new LinkedList<>();
                        for(int i=key; i<key+interval.period; i+=mRule.interval.period)
                            objects.add(i);

                        fetchNonGroupingTimeSeries(result, mRule, objects, aggAnalysis);
                        long val = mRule.type==AggregationType.MINIMUM_X ? Integer.MAX_VALUE : Integer.MIN_VALUE;
                        for(String field : result.getFieldNames())
                            if(mRule.type==AggregationType.MINIMUM_X)
                                val = Math.min(result.getNumber(field).longValue(), val);
                            else
                                val = Math.max(result.getNumber(field).longValue(), val);

                        calculatedResult.putNumber(key+"000", val);
                    }

                    break;
                case AVERAGE_X:

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
                    for(Integer key : keys) {
                        JsonObject result = new JsonObject();
                        LinkedList<Integer> objects = new LinkedList<>();
                        for(int i=key; i<key+interval.period; i+=mRule.interval.period)
                            objects.add(i);

                        fetchGroupingTimeSeries(result, mRule, objects, aggAnalysis, items);
                        HashMap<String, Object> sum = new HashMap<>();
                        for(String timestamp : result.getFieldNames()) {
                            JsonObject timestampItems = result.getObject(timestamp);
                            for(String item : timestampItems.getFieldNames()) {
                                Long existingCounter = (Long) sum.getOrDefault(item, 0L);
                                Long newCounter = timestampItems.getLong(item);
                                sum.put(item, existingCounter+newCounter);
                            }
                        }
                        calculatedResult.putObject(key+"000", new JsonObject(sum));

                    }
                    break;
                case MINIMUM_X:
                case MAXIMUM_X:
                    for(Integer key : keys) {
                        JsonObject result = new JsonObject();
                        LinkedList<Integer> objects = new LinkedList<>();
                        for(int i=key; i<key+interval.period; i+=mRule.interval.period)
                            objects.add(i);

                        fetchNonGroupingTimeSeries(result, mRule, objects, aggAnalysis);
                        long val = mRule.type==AggregationType.MINIMUM_X ? Integer.MAX_VALUE : Integer.MIN_VALUE;
                        for(String field : result.getFieldNames())
                            if(mRule.type==AggregationType.MINIMUM_X)
                                val = Math.min(result.getNumber(field).longValue(), val);
                            else
                                val = Math.max(result.getNumber(field).longValue(), val);

                        calculatedResult.putNumber(key+"000", val);
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

    private JsonObject fetch(AggregationRule mRule, JsonObject query, AggregationAnalysis aggAnalysis) {
        JsonObject result = new JsonObject();
        Integer items;
        try {
            items = toInt(query.getField("items"), 10);
        } catch (IllegalArgumentException e) {
            return returnError("items parameter is not numeric");
        }

        if (mRule instanceof MetricAggregationRule) {
            if (mRule.groupBy != null) {
                fetchGroupingMetric(result, mRule, aggAnalysis, items);
            } else {
                fetchNonGroupingMetric(result, mRule, aggAnalysis);
            }
        } else if (mRule instanceof TimeSeriesAggregationRule) {
            Collection<Integer> keys;
            try {
                keys = getTimeFrame(query.getField("frame"), ((TimeSeriesAggregationRule) mRule).interval, query.getString("start"), query.getString("end"));
            } catch (IllegalArgumentException e) {
                return returnError(e.getMessage());
            }

            if (mRule.groupBy == null)
                fetchNonGroupingTimeSeries(result, (TimeSeriesAggregationRule) mRule, keys, aggAnalysis);
            else
                fetchGroupingTimeSeries(result, (TimeSeriesAggregationRule) mRule, keys, aggAnalysis, items);
        }

        JsonObject json = new JsonObject().putObject("result", result);
        if (!mRule.batch_status && mRule.strategy != AnalysisRuleStrategy.REAL_TIME)
            json.putString("info", "batch results are not combined yet");

        return json;
    }


    public static void fetchNonGroupingMetric(JsonObject json, AggregationRule rule, AggregationAnalysis aggAnalysis) {
        String rule_id = rule.id;
        switch (aggAnalysis) {
            case COUNT:
            case COUNT_X:
            case SUM_X:
            case MAXIMUM_X:
            case MINIMUM_X:
                json.putNumber("result", databaseAdapter.getCounter(rule_id));
                break;
            case AVERAGE_X:
                json.putNumber("result", cacheAdapter.getAverageCounter(rule_id).getValue());
                break;
            case COUNT_UNIQUE_X:
                json.putNumber("result", databaseAdapter.getSetCount(rule_id));
                break;
            case SELECT_UNIQUE_X:
                json.putArray("result", new JsonArray(cacheAdapter.getSet(rule_id).toArray()));
                break;
        }
    }

    public static void fetchGroupingMetric(JsonObject json, AggregationRule rule, AggregationAnalysis aggAnalysis, Integer items) {
        String rule_id = rule.id;

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
                for (Map.Entry<String, Long> item : cacheAdapter.getGroupByCounters(rule_id).entrySet()) {
                    json.putNumber(item.getKey(), item.getValue());
                }

                break;
            case COUNT_UNIQUE_X:
                for(Map.Entry<String, Long> item : cacheAdapter.getGroupByStringsCounts(rule_id, items).entrySet()) {
                    json.putNumber(item.getKey(), item.getValue());
                }
                break;
            case SELECT_UNIQUE_X:
                for(Map.Entry<String, Set<String>> item : cacheAdapter.getGroupByStrings(rule_id, items).entrySet()) {
                    json.putArray(item.getKey(), new JsonArray(item.getValue().toArray()));
                }
                break;
        }

    }

    public static void fetchNonGroupingTimeSeries(JsonObject results, TimeSeriesAggregationRule rule, Collection<Integer> keys, AggregationAnalysis aggAnalysis) {
        String rule_id = rule.id;
        int now = rule.interval.spanCurrent().current();

        switch (aggAnalysis) {
            case SELECT_UNIQUE_X:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putArray(time + "000", new JsonArray(adapter.getSet(rule_id + ":" + time).toArray()));
                }
                break;
            case COUNT_UNIQUE_X:
                for (Integer time : keys) {
                    CacheAdapter    adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(time + "000", adapter.getSetCount(rule_id + ":" + time));
                }
                break;
            case AVERAGE_X:
                for (Integer time : keys) {
                    CacheAdapter    adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(time + "000", adapter.getAverageCounter(rule_id + ":" + time).getValue());
                }
                break;
            default:
                for (Integer time : keys) {
                    CacheAdapter adapter = now == time ? cacheAdapter : databaseAdapter;
                    results.putNumber(time + "000", adapter.getCounter(rule_id + ":" + time));
                }
                break;
        }
    }

    public static void fetchGroupingTimeSeries(JsonObject results, TimeSeriesAggregationRule rule, Collection<Integer> keys, AggregationAnalysis aggAnalysis, Integer items) {
        String rule_id = rule.id;
        int now = rule.interval.spanCurrent().current();

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
    }

    public static List<Integer> getTimeFrame(Object items_obj, SpanTime interval, String start, String end) throws IllegalArgumentException {
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
            s_timestamp = parseDate(start);
            if (s_timestamp == null) {
                throw new IllegalArgumentException("couldn't parse start time.");
            }
        }

        if (end != null) {
            e_timestamp = parseDate(end);
            if (e_timestamp == null) {
                throw new IllegalArgumentException("couldn't parse end time.");
            }
        }

        if (s_timestamp != null && e_timestamp != null) {
            SpanTime starting_point = interval.span(s_timestamp);
            int ending_point = interval.span(e_timestamp).current();
            if (starting_point.current() > ending_point) {
                throw new IllegalArgumentException("start time must be lower than end time.");
            }

            if (starting_point.untilTimeFrame(ending_point) > 100) {
                throw new IllegalArgumentException("there is more than 100 items between start and times.");
            }

            while (true) {
                keys.add(starting_point.current());
                starting_point = starting_point.getNext();
                if (starting_point.current() > ending_point)
                    break;
            }

        } else if (s_timestamp != null && items_obj != null) {
            SpanTime starting_point = interval.span(s_timestamp);
            int ending_point = (int) (System.currentTimeMillis() / 1000);
            if (starting_point.current() > ending_point) {
                throw new IllegalArgumentException("start time must be lower than end time.");
            }

            if (starting_point.untilTimeFrame(ending_point) > 100) {
                throw new IllegalArgumentException("there is more than 100 items between start and times.");
            }

            for (int i = 0; i < items; i++) {
                keys.add(starting_point.current());
                starting_point = starting_point.getNext();
                if (starting_point.current() > ending_point)
                    break;
            }
        } else if (e_timestamp != null && items_obj != null) {
            SpanTime ending_point = interval.span(e_timestamp);

            for (int i = 1; i < items; i++) {
                keys.add(ending_point.current());
                ending_point = ending_point.getPrevious();
            }
        } else if (items_obj != null) {
            keys.add(interval.spanCurrent().current());
            for (int i = 1; i < items; i++) {
                interval = interval.getPrevious();
                keys.addFirst(interval.current());
            }
        } else {
            throw new IllegalArgumentException("time frame is invalid. usage: [start, end], [start, frame], [end, frame], [frame].");
        }
        return keys;
    }
}
