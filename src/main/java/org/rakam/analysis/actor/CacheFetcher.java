package org.rakam.analysis.actor;

import org.rakam.analysis.model.AggregationRule;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 29/03/14.
 */
public class CacheFetcher {
    private static final HazelcastCacheAdapter cacheAdapter = new HazelcastCacheAdapter();

    public static JsonObject fetch(AggregationRule rule, JsonObject query) {
        return null;
    }
}
