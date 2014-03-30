package org.rakam.analysis;

import com.hazelcast.core.IMap;
import org.rakam.analysis.event.CacheFetcher;
import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.AggregationRuleList;
import org.rakam.cache.CacheAdapterFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class AnalysisVerticle extends Verticle {
    private static IMap<String, AggregationRuleList> map;


    public void start() {
        map = CacheAdapterFactory.getAggregationMap();

        vertx.eventBus().registerHandler("analysis", new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                JsonObject query = message.body();
                String rid = query.getString("_rule");
                String tracker = query.getString("tracker");
                AggregationRuleList rules = map.get(tracker);
                    if (rules==null) {
                    JsonObject j = new JsonObject();
                    j.putString("error", "tracker_id is not exist.");
                    message.reply(j);
                    return;
                }

                for(AggregationRule rule : rules) {
                    if(rule.id().equals(rid)) {
                        message.reply(CacheFetcher.fetch(rule, query));
                        return;
                    }
                }

                JsonObject j = new JsonObject();
                j.putString("error", "aggregation rule couldn't found. you have to create rule in order the perform this query.");
                message.reply(j);
                return;
            }
        });
    }
}