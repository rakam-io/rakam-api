package org.rakam.analysis;

import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.util.SpanDateTime;
import org.vertx.java.core.json.JsonObject;

import java.util.LinkedList;
import java.util.UUID;

/**
 * Created by buremba on 15/01/14.
 */
public class PreAggregationFetcher {
    private static final HazelcastCacheAdapter cacheAdapter = new HazelcastCacheAdapter();

    public static JsonObject fetch(AggregationRule rule, JsonObject query) {
        switch(rule.getType()) {
            case ANALYSIS_METRIC:
                return fetchMetric(rule.id, rule.groupBy);
            case ANALYSIS_TIMESERIES:
                Integer last;
                try {
                    last = Integer.parseInt(query.getString("frame"));
                } catch (NumberFormatException e) {
                    JsonObject j = new JsonObject();
                    j.putString("error", "time frame parameter is required.");
                    return j;
                }
                return fetchTimeSeries(rule.id, ((TimeSeriesAggregationRule) rule).interval, last, rule.groupBy);
            default:
                // TODO: log
                return null;
        }

    }

    public static JsonObject fetchMetric(UUID id, String groupBy) {
        JsonObject json = new JsonObject();
        json.putString("_uuid", id.toString());
        json.putNumber("result", cacheAdapter.getCounter(id.toString()));
        return json;
    }

    public static JsonObject fetchTimeSeries(UUID id, SpanDateTime current, int numberOfItems, String groupBy) {
        JsonObject json = new JsonObject();
        String uuid_str = id.toString();
        json.putString("_uuid", uuid_str);

        LinkedList<Long> keys = new LinkedList();
        keys.add(current.spanCurrentTimestamp().getMillis());
        for(int i=0; i<numberOfItems; i++) {
            current = current.getPrevious();
            keys.addFirst(current.spanCurrentTimestamp().getMillis());
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
