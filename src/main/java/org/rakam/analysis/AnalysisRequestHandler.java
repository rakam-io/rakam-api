package org.rakam.analysis;

import com.hazelcast.core.IMap;
import org.rakam.analysis.rule.AnalysisRule;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.GaugeRule;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.CacheAdapterFactory;
import org.rakam.constant.AggregationType;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;
import org.rakam.util.SpanTime;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by buremba on 07/05/14.
 */
public class AnalysisRequestHandler implements Handler<Message<JsonObject>> {
    private static final DatabaseAdapter databaseAdapter = new CassandraAdapter();
    IMap<String, AnalysisRuleList> cacheAggs = CacheAdapterFactory.getAggregationMap();


    @Override
    public void handle(Message<JsonObject> event) {
        JsonObject query = event.body();
        String rid = query.getString("id");
        String tracker = query.getString("tracker");
        if(tracker==null) {
            JsonObject j = new JsonObject();
            j.putString("error", "tracker parameter is required.");
            event.reply(j);
            return;
        }

        AnalysisRuleList rules = cacheAggs.get(tracker);
        if (rules==null) {
            JsonObject j = new JsonObject();
            j.putString("error", "tracker id is not exist.");
            event.reply(j);
            return;
        }

        for(AnalysisRule rule : rules) {
            if(rule.id.equals(rid)) {
                event.reply(fetch(rule, query));
                return;
            }
        }

        JsonObject j = new JsonObject();
        j.putString("error", "aggregation rule couldn't found. you have to create rule in order the perform this query.");
        event.reply(j);
        return;
    }
    public static JsonObject fetch(AnalysisRule rule, JsonObject query) {
        JsonObject json = new JsonObject();
        json.putString("_rule", rule.id);

        switch(rule.analysisType()) {
            case ANALYSIS_GAUGE:
                return fetchGauge(json, (GaugeRule)rule);
            case ANALYSIS_METRIC:
                AggregationRule aggRule = (AggregationRule) rule;
                return aggRule.groupBy!=null ? fetchGroupingMetric(json, aggRule, query) : fetchMetric(json, aggRule, query);
            case ANALYSIS_TIMESERIES:
                Integer items = null;
                String items_str = query.getString("items");
                String start = query.getString("start");
                String end = query.getString("end");

                if (items_str!=null)
                    try {
                        items = Integer.parseInt(items_str);
                    } catch (NumberFormatException e) {
                        JsonObject j = new JsonObject();
                        j.putString("error", "time frame parameter is required.");
                        return j;
                    }

                return fetchTimeSeries(json, ((TimeSeriesAggregationRule) rule), start, end, items, query);
            default:
                // TODO: log
                return null;
        }

    }

    private static JsonObject fetchGauge(JsonObject json, GaugeRule rule) {
        return null;
    }

    public static JsonObject fetchMetric(JsonObject json, AggregationRule rule, JsonObject query) {
        String rule_id = rule.id;

        if (rule.type== AggregationType.COUNT || rule.type== AggregationType.COUNT_X ||
                rule.type==AggregationType.SUM_X || rule.type==AggregationType.MAXIMUM_X || rule.type==AggregationType.MINIMUM_X) {
            json.putNumber("result", databaseAdapter.getCounter(rule_id));
        } else
        if (rule.type==AggregationType.AVERAGE_X) {
            long r = 0;
            Long counter = databaseAdapter.getCounter(MetricAggregationRule.buildId(rule.project, AggregationType.COUNT_X, rule.select, rule.filters, rule.groupBy));
            if(counter>0)
                r = databaseAdapter.getCounter(rule_id)/counter;
            json.putNumber("result", r);
        }else
        if(rule.type==AggregationType.COUNT_UNIQUE_X) {
            json.putNumber("result", databaseAdapter.getSetCount(rule_id));
        }else
        if(rule.type==AggregationType.SELECT_UNIQUE_Xs) {
            selectUnique(json, rule_id, query);
        }
        return json;

    }

    public static void selectUnique(JsonObject json, String rule_id, JsonObject query) {
        JsonArray arr = new JsonArray();
        Iterator<String> c = databaseAdapter.getSetIterator(rule_id);

        String offset = query.getString("offset");
        int count = 0;
        Integer lim = query.getInteger("limit");
        int limit = Math.min(lim!=null ? lim : 10000, 10000);
        while(c.hasNext() && count<limit) {
            arr.add(c.next());
            count++;
        }

        json.putArray("result", arr);
        json.putNumber("count", count);
    }

    public static JsonObject selectUniqueGrouping(String rule_id, JsonObject query) {
        JsonObject resobj = new JsonObject();
        Iterator list = databaseAdapter.getSetIterator(rule_id + ":keys");
        while(list.hasNext()) {
            String item = (String) list.next();

            JsonArray arr = new JsonArray();
            Iterator<String> c = databaseAdapter.getSetIterator(rule_id+":"+item);

            String offset = query.getString("offset");
            int count = 0;
            Integer lim = query.getInteger("limit");
            int limit = Math.min(lim!=null ? lim : 1000, 10000);
            while(c.hasNext() && count<limit) {
                arr.add(c.next());
                count++;
            }
            resobj.putArray(item, arr);
        }
        return resobj;
    }

    public static JsonObject fetchGroupingMetric(JsonObject json, AggregationRule rule, JsonObject query) {
        String rule_id = rule.id;

        if (rule.type==AggregationType.COUNT || rule.type== AggregationType.COUNT_X ||
                rule.type==AggregationType.SUM_X || rule.type==AggregationType.MAXIMUM_X || rule.type==AggregationType.MINIMUM_X) {
            JsonObject arr = new JsonObject();
            json.putObject("result", arr);
            Iterator list = databaseAdapter.getSetIterator(rule_id + ":keys");
            while(list.hasNext()) {
                String item = (String) list.next();
                arr.putNumber(item, databaseAdapter.getSetCount(item + ":" + item));
            }
        } else
        if (rule.type==AggregationType.AVERAGE_X) {
            JsonObject arr = new JsonObject();
            json.putObject("result", arr);
            Iterator list = databaseAdapter.getSetIterator(rule_id + ":keys");
            long r = 0;
            while(list.hasNext()) {
                String item = (String) list.next();
                Long counter = databaseAdapter.getCounter(MetricAggregationRule.buildId(rule.project, AggregationType.COUNT_X, rule.select, rule.filters, rule.groupBy)+":"+item);
                if(counter>0)
                    r = databaseAdapter.getSetCount(rule_id+":"+list)/counter;
                arr.putNumber(item, r);
            }
        }else
        if(rule.type==AggregationType.COUNT_UNIQUE_X) {
            JsonObject arr = new JsonObject();
            json.putObject("result", arr);
            Iterator list = databaseAdapter.getSetIterator(rule_id + ":keys");
            while(list.hasNext()) {
                String item = (String) list.next();
                arr.putNumber(item, databaseAdapter.getSetCount(rule_id));
            }

        }else
        if(rule.type==AggregationType.SELECT_UNIQUE_Xs) {
            selectUniqueGrouping(rule_id, query);
        }
        return json;

    }


    public static JsonObject fetchTimeSeries(JsonObject json, TimeSeriesAggregationRule rule, String start, String end, Integer items, JsonObject query)  {
        String rule_id = rule.id;
        SpanTime interval = rule.interval;

        LinkedList<Integer> keys = new LinkedList();

        if(items!=null && items>100) {
            JsonObject j = new JsonObject();
            j.putString("error", "items must be lower than 100");
            return j;
        }

        Integer s_timestamp = null;
        Integer e_timestamp = null;
        if(start!=null) {
            s_timestamp = parseDate(start);
            if(s_timestamp==null) {
                JsonObject j = new JsonObject();
                j.putString("error", "couldn't parse start time.");
                return j;
            }
        }

        if(end!=null) {
            e_timestamp = parseDate(end);
            if(e_timestamp==null) {
                JsonObject j = new JsonObject();
                j.putString("error", "couldn't parse end time.");
                return j;
            }
        }

        if(s_timestamp!=null && e_timestamp!=null) {
            SpanTime starting_point = interval.span(s_timestamp);
            int ending_point = interval.span(e_timestamp).current();
            if (starting_point.current() > ending_point) {
                JsonObject j = new JsonObject();
                j.putString("error", "start time must be lower than end time.");
                return j;
            }

            if(starting_point.untilTimeFrame(ending_point)>100) {
                JsonObject j = new JsonObject();
                j.putString("error", "there is more than 100 items between start and times.");
                return j;
            }

            while(true) {
                keys.add(starting_point.current());
                starting_point = starting_point.getNext();
                if(starting_point.current() > ending_point)
                    break;
            }

        }else
        if(s_timestamp!=null && items!=null) {
            SpanTime starting_point = interval.span(s_timestamp);
            int ending_point = (int) (System.currentTimeMillis()/1000);
            if (starting_point.current() > ending_point) {
                JsonObject j = new JsonObject();
                j.putString("error", "start time must be lower than end time.");
                return j;
            }

            if(starting_point.untilTimeFrame(ending_point)>100) {
                JsonObject j = new JsonObject();
                j.putString("error", "there is more than 100 items between start and times.");
                return j;
            }

            for(int i=0; i<items; i++) {
                keys.add(starting_point.current());
                starting_point = starting_point.getNext();
                if(starting_point.current() > ending_point)
                    break;
            }
        }else
        if(e_timestamp!=null && items!=null) {
            SpanTime ending_point = interval.span(e_timestamp);

            for(int i=0; i<items; i++) {
                keys.add(ending_point.current());
                ending_point = ending_point.getPrevious();
            }
        }else
        if(items!=null) {
            keys.add(interval.spanCurrent().current());
            for(int i=0; i<items; i++) {
                interval = interval.getPrevious();
                keys.addFirst(interval.current());
            }
        }else {
            JsonObject j = new JsonObject();
            j.putString("error", "time frame is invalid. usage: [start, end], [start, items], [end, items], [items].");
            return j;
        }



        JsonObject results;
        if(rule.type==AggregationType.SELECT_UNIQUE_Xs) {
            results = new JsonObject();

            if (rule.groupBy==null)
                for(Integer time: keys) {
                    JsonObject j = new JsonObject();
                    selectUnique(j, rule_id+":"+time, query);
                    results.putObject(time.toString(), j);
                }
            else
                for(Integer time: keys) {
                    results.putObject(time.toString(), selectUniqueGrouping(rule_id+":"+time, query));
                }


        }else {

            if (rule.groupBy==null) {
                Iterator<Integer> it = keys.iterator();
                JsonObject j = new JsonObject();
                for(Integer cabin: keys) {
                    j.putNumber(String.valueOf(((long)cabin)*1000), databaseAdapter.getCounter(rule_id+":"+cabin));
                }
                results = j;
            } else {
                JsonObject j = new JsonObject();
                for(Integer cabin: keys) {
                    JsonArray arr = new JsonArray();
                    for(String item : databaseAdapter.getSet(rule_id+":"+cabin))
                        arr.addString(item);
                    j.putArray(String.valueOf(((long)cabin)*1000), arr);
                }
                results = j;
            }
        }

        json.putObject("result", results);
        if(!rule.batch_status && rule.strategy!=AnalysisRuleStrategy.REAL_TIME) {
            json.putString("info", "batch results are not combined yet");
        }
        return json;
    }

    public static Integer parseDate(String str) {
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            try {
                return (int) (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime()/1000);
            } catch (ParseException ex) {
                return null;
            }
        }
    }
}
