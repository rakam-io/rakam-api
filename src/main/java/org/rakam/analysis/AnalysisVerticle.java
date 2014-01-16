package org.rakam.analysis;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.IMap;
import org.rakam.analysis.model.AggregationRule;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.List;
import java.util.UUID;

public class AnalysisVerticle extends Verticle {
    private static IMap<String, List<AggregationRule>> map;


    public void start() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("analytics").setPassword("");
        map =  HazelcastClient.newHazelcastClient(clientConfig).getMap("aggregation.rules");

        vertx.eventBus().registerHandler("analysis", new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> message) {
                JsonObject query = message.body();
                UUID uuid;
                try {
                    uuid = UUID.fromString(query.getString("_uuid"));
                } catch(IllegalArgumentException e){
                    JsonObject j = new JsonObject();
                    j.putString("error", "_uuid is not valid.");
                    message.reply(j);
                    return;
                }
                String tracker = query.getString("tracker");
                List<AggregationRule> rules = map.get(tracker);
                if (rules==null) {
                    JsonObject j = new JsonObject();
                    j.putString("error", "tracker_id is not exist.");
                    message.reply(j);
                    return;
                }

                for(AggregationRule rule : rules) {
                    if(rule.id.equals(uuid))
                        message.reply(PreAggregationFetcher.fetch(rule, query));
                }

                JsonObject j = new JsonObject();
                j.putString("error", "aggregation rule couldn't found.");
                message.reply(j);
                return;
            }
        });
    }
}