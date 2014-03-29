package org.rakam.analysis.fetch;

import org.joda.time.DateTime;
import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.constant.AggregationType;
import org.rakam.util.SpanDateTime;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by buremba on 15/01/14.
 */
public class CacheFetcher {
    private static final HazelcastCacheAdapter cacheAdapter = new HazelcastCacheAdapter();

    public static JsonObject fetch(AggregationRule rule, JsonObject query) {
        switch(rule.analysisType()) {
            case ANALYSIS_METRIC:
                return rule.groupBy!=null ? fetchGroupingMetric(rule, query) : fetchMetric(rule, query);
            case ANALYSIS_TIMESERIES:
                Integer items = null;
                String items_str = query.getString("start");
                String start = query.getString("frame");
                String end = query.getString("end");

                if (items_str!=null)
                    try {
                        items = Integer.parseInt(items_str);
                    } catch (NumberFormatException e) {
                        JsonObject j = new JsonObject();
                        j.putString("error", "time frame parameter is required.");
                        return j;
                    }

                return fetchTimeSeries(rule.id(), ((TimeSeriesAggregationRule) rule).interval, start, end, items, rule.groupBy);
            default:
                // TODO: log
                return null;
        }

    }

    public static JsonObject fetchMetric(AggregationRule rule, JsonObject query) {
        JsonObject json = new JsonObject();
        json.putString("_rule", rule.id());
        if (rule.type==AggregationType.COUNT || rule.type== AggregationType.COUNT_X ||
                rule.type==AggregationType.SUM_X || rule.type==AggregationType.MAXIMUM_X || rule.type==AggregationType.MINIMUM_X) {
            json.putNumber("result", cacheAdapter.getCounter(rule.id()));
        } else
        if (rule.type==AggregationType.AVERAGE_X) {
            long r = 0;
            Long counter = cacheAdapter.getCounter(rule.buildId(rule.project, AggregationType.COUNT_X, rule.select, rule.filters, rule.groupBy));
            if(counter>0)
                r = cacheAdapter.getCounter(rule.id())/counter;
            json.putNumber("result", r);
        }else
        if(rule.type==AggregationType.COUNT_UNIQUE_X) {
            json.putNumber("result", cacheAdapter.getSetCount(rule.id()));
        }else
        if(rule.type==AggregationType.SELECT_UNIQUE_Xs) {
            JsonArray arr = new JsonArray();
            Iterator<String> c = cacheAdapter.getSetIterator(rule.id());

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
        return json;

    }

    public static JsonObject fetchGroupingMetric(AggregationRule rule, JsonObject query) {
        JsonObject json = new JsonObject();
        json.putString("_rule", rule.id());
        if (rule.type==AggregationType.COUNT || rule.type== AggregationType.COUNT_X ||
                rule.type==AggregationType.SUM_X || rule.type==AggregationType.MAXIMUM_X || rule.type==AggregationType.MINIMUM_X) {
            JsonObject arr = new JsonObject();
            json.putObject("result", arr);
            Iterator list = cacheAdapter.getSetIterator(rule.id() + ":keys");
            while(list.hasNext()) {
                String item = (String) list.next();
                arr.putNumber(item, cacheAdapter.getSetCount(item+":"+item));
            }
        } else
        if (rule.type==AggregationType.AVERAGE_X) {
            JsonObject arr = new JsonObject();
            json.putObject("result", arr);
            Iterator list = cacheAdapter.getSetIterator(rule.id() + ":keys");
            long r = 0;
            while(list.hasNext()) {
                String item = (String) list.next();
                Long counter = cacheAdapter.getCounter(rule.buildId(rule.project, AggregationType.COUNT_X, rule.select, rule.filters, rule.groupBy)+":"+item);
                if(counter>0)
                    r = cacheAdapter.getSetCount(rule.id()+":"+list)/counter;
                arr.putNumber(item, r);
            }
        }else
        if(rule.type==AggregationType.COUNT_UNIQUE_X) {
            JsonObject arr = new JsonObject();
            json.putObject("result", arr);
            Iterator list = cacheAdapter.getSetIterator(rule.id() + ":keys");
            while(list.hasNext()) {
                String item = (String) list.next();
                arr.putNumber(item, cacheAdapter.getSetCount(rule.id()));
            }

        }else
        if(rule.type==AggregationType.SELECT_UNIQUE_Xs) {
            JsonObject resobj = new JsonObject();
            json.putObject("result", resobj);
            Iterator list = cacheAdapter.getSetIterator(rule.id() + ":keys");
            while(list.hasNext()) {
                String item = (String) list.next();

                JsonArray arr = new JsonArray();
                Iterator<String> c = cacheAdapter.getSetIterator(rule.id()+":"+item);

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
        }
        return json;

    }

    public static JsonObject fetchTimeSeries(String id, SpanDateTime interval, String start, String end, Integer items, String groupBy) {
        JsonObject json = new JsonObject();
        json.putString("_rule", id);

        LinkedList<Long> keys = new LinkedList();

        if(items>100) {
            JsonObject j = new JsonObject();
            j.putString("error", "items must be lower than 100");
            return j;
        }

        if(start!=null && end!=null) {
            SpanDateTime starting_point = interval.spanTimestamp(start);
            DateTime ending_point = interval.spanTimestamp(end).getDateTime();
            if (starting_point.getDateTime().isAfter(ending_point)) {
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
                keys.add(starting_point.getDateTime().getMillis());
                starting_point = starting_point.getNext();
                if(starting_point.getDateTime().isAfter(ending_point))
                    break;
            }

        }else
        if(start!=null && items!=null) {
            SpanDateTime starting_point = interval.spanTimestamp(start);
            DateTime ending_point = DateTime.now();
            if (starting_point.getDateTime().isAfter(ending_point)) {
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
                keys.add(starting_point.getDateTime().getMillis());
                starting_point = starting_point.getNext();
                if(starting_point.getDateTime().isAfter(ending_point))
                    break;
            }
        }else
        if(end!=null && items!=null) {
            SpanDateTime ending_point = interval.spanTimestamp(end);

            for(int i=0; i<items; i++) {
                keys.add(ending_point.getDateTime().getMillis());
                ending_point = ending_point.getPrevious();
            }
        }else
        if(items!=null) {
            keys.add(interval.spanCurrentTimestamp().getDateTime().getMillis());
            for(int i=0; i<items; i++) {
                interval = interval.getPrevious();
                keys.addFirst(interval.spanCurrentTimestamp().getDateTime().getMillis());
            }
        }else {
            JsonObject j = new JsonObject();
            j.putString("error", "time frame is invalid. Available pairs: [start, end], [start, items], [end, items], [items].");
            return j;
        }



        JsonObject results;
        if (groupBy==null)
            results = cacheAdapter.getMultiCounts(id, keys);
        else
            results = cacheAdapter.getMultiSetCounts(id, keys);

        json.putObject("result", results);
        return json;
    }


}
