package org.rakam.analysis;

import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.MetricAggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.util.SpanDateTime;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by buremba on 15/01/14.
 */
public class PreAggregationFetcher {
    private static final HazelcastCacheAdapter cacheAdapter = new HazelcastCacheAdapter();

    public static JsonObject fetch(AggregationRule rule, JsonObject query) {
        if (rule instanceof MetricAggregationRule)
            return fetchMetric(rule.id, false);
        else if(rule instanceof TimeSeriesAggregationRule) {
            Integer last;
            try {
                last = Integer.parseInt(query.getString("frame"));
            } catch (NumberFormatException e) {
                JsonObject j = new JsonObject();
                j.putString("error", "time frame parameter is required.");
                return j;
            }
            return fetchTimeSeries(rule.id, ((TimeSeriesAggregationRule) rule).interval, last, false);
        }
        else {
            // TODO: log
            return null;
        }
    }

    public static JsonObject fetchMetric(UUID id, boolean hasGrouping) {
        JsonObject json = new JsonObject();
        json.putString("_uuid", id.toString());
        json.putNumber("result", cacheAdapter.getCounter(id.toString()));
        return json;
    }

    public static JsonObject fetchTimeSeries(UUID id, SpanDateTime current, int numberOfItems, boolean hasGrouping) {
        JsonObject json = new JsonObject();
        String uuid_str = id.toString();
        json.putString("_uuid", uuid_str);

        List<Long> keys = new ArrayList();

        keys.add(current.spanCurrentTimestamp().getMillis());
        for(int i=0; i<numberOfItems; i++) {
            current = current.getPrevious();
            keys.add(current.spanCurrentTimestamp().getMillis());
        }

        Map<Long,Integer> results = cacheAdapter.getMultiCounts(id, keys);

        JsonObject result = new JsonObject();
        json.putObject("result", result);
        for(Map.Entry<Long, Integer> r : results.entrySet())
            result.putNumber(String.valueOf(r.getKey()), r.getValue());

        return json;
    }


}
