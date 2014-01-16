package org.rakam.worker.aggregation;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.cache.SimpleCacheAdapter;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;
import org.rakam.model.Actor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.*;

public class AggregationLogic extends Verticle implements Handler<Message<JsonObject>> {
    private EventBus eb;
    private String address = UUID.randomUUID().toString();
    private Map<UUID, List<AggregationRule>> preAggregation;
    private SimpleCacheAdapter cacheAdapter;
    private DatabaseAdapter databaseAdapter;

    @Override
    public void start() {
        eb = vertx.eventBus();
        eb.registerHandler(address, this);
        JsonObject msg = new JsonObject().putString("processor", address);
        eb.send("aggregation.orderQueue.register", msg);
        cacheAdapter = new HazelcastCacheAdapter();
        databaseAdapter = new CassandraAdapter();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("analytics").setPassword("");
        preAggregation =  HazelcastClient.newHazelcastClient(clientConfig).getMap("aggregation.rules");
    }


    @Override
    public void stop() {
        JsonObject msg = new JsonObject().putString("processor", address);
        eb.send("aggregation.orderQueue.unregister", msg);
        eb.unregisterHandler(address, this);
    }

    public void handle(Message<JsonObject> m) {
        JsonObject message = m.body();
        String project = message.getString("tracker");
        String actor_id = message.getString("actor");

        Actor actor = null;
        if (actor_id!=null) {
            JsonObject actor_cache_props = cacheAdapter.getActorProperties(project, actor_id);
            if(actor_cache_props==null) {
                actor = databaseAdapter.getActor(project, actor_id);
                cacheAdapter.addActorProperties(project, actor_id, actor.properties);
            } else {
                actor = new Actor(project, actor_id, actor_cache_props);
            }
        }

        int time_cabin = Calendar.getInstance().get(Calendar.MONTH);
        databaseAdapter.addEvent(project, time_cabin, actor_id, message.encode().getBytes());
        aggregate(message, actor);
        m.reply();
    }

    /*
        Find pre-aggregation rules and match with the event.
        If it matches update the appropriate counter.
    */
    public void aggregate(JsonObject m, Actor actor) {

        List<AggregationRule> aggregations = preAggregation.get(m.getString("tracker"));

        if(aggregations==null)
            return;

        it:
        for(AggregationRule aggregation : aggregations) {
            HashMap<String, String> filters = aggregation.filters;
            if (filters!=null) {
                for(Map.Entry<String, String> filter : filters.entrySet())
                    if (!filter.getValue().equals(m.getString(filter.getKey())))
                        continue it;
            }
            String groupBy = aggregation.groupBy;
            switch (aggregation.getType()) {
                case ANALYSIS_METRIC:
                    if(groupBy==null)
                        cacheAdapter.increment(aggregation.id);
                    else {
                        cacheAdapter.addToGroupByItem(aggregation.id, groupBy, m.getString(groupBy));
                    }
                    break;
                case ANALYSIS_TIMESERIES:
                    if(groupBy==null)
                        cacheAdapter.increment(aggregation.id, ((TimeSeriesAggregationRule) aggregation).interval.spanCurrentTimestamp());
                    else
                        cacheAdapter.addToGroupByItem(aggregation.id, ((TimeSeriesAggregationRule) aggregation).interval.spanCurrentTimestamp(), groupBy, m.getString(groupBy));
                    break;
            }
        }

    }

}