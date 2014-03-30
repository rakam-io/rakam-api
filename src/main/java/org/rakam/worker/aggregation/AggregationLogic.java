package org.rakam.worker.aggregation;

import com.hazelcast.core.IMap;
import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.AggregationRuleList;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.cache.CacheAdapterFactory;
import org.rakam.cache.SimpleCacheAdapter;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.cassandra.CassandraAdapter;
import org.rakam.model.Actor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AggregationLogic extends Verticle implements Handler<Message<JsonObject>> {
    private EventBus eb;
    private String address = UUID.randomUUID().toString();
    private IMap<String, AggregationRuleList> preAggregation;
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

        preAggregation = CacheAdapterFactory.getAggregationMap();
    }


    @Override
    public void stop() {
        JsonObject msg = new JsonObject().putString("processor", address);
        eb.send("aggregation.orderQueue.unregister", msg);
        eb.unregisterHandler(address, this);
    }

    public void handle(Message<JsonObject> m) {
        JsonObject message = m.body();
        String project = message.getString("_tracker");
        String actor_id = message.getString("_user");


         /*
            The current implementation change cabins monthly.
            However the interval must be determined by the system by taking account of data frequency
         */
        int time_cabin = Calendar.getInstance().get(Calendar.MONTH);
        databaseAdapter.addEvent(project, time_cabin, actor_id, message.encode().getBytes());

        Actor actor = null;
        if (actor_id != null) {
            JsonObject actor_cache_props = cacheAdapter.getActorProperties(project, actor_id);
            if (actor_cache_props == null) {
                actor = databaseAdapter.getActor(project, actor_id);
                if(actor==null) {
                    actor = databaseAdapter.createActor(project, actor_id, null);
                }
                cacheAdapter.setActorProperties(project, actor_id, actor.properties);
            } else {
                actor = new Actor(project, actor_id, actor_cache_props);
            }
        }


        aggregate(project, message, actor);
        JsonObject a = new JsonObject();
        a.putBoolean("success", true);
        m.reply(a);
    }

    /*
        Find pre-aggregation rules and match with the event.
        If it matches update the appropriate counter.
    */
    public void aggregate(String project, JsonObject m, Actor actor) {

        AggregationRuleList aggregations = preAggregation.get(project);

        if (aggregations == null)
            return;

        it:
        for (AggregationRule aggregation : aggregations) {
            HashMap<String, String> filters = aggregation.filters;
            if (filters != null) {
                for (Map.Entry<String, String> filter : filters.entrySet())
                    if (!filter.getValue().equals(m.getString(filter.getKey())))
                        continue it;
            }
            String groupBy = aggregation.groupBy;
            String key = aggregation.id();
            if (aggregation.analysisType() == Analysis.ANALYSIS_TIMESERIES)
                key += ":" + ((TimeSeriesAggregationRule) aggregation).interval.spanCurrentTimestamp().getDateTime().getMillis();


            if (groupBy == null) {
                aggregateByNonGrouping(key, m.getString(aggregation.select), aggregation.type, actor);
            } else {
                String item = m.getString(groupBy);
                if (item != null)
                    aggregateByGrouping(key, m.getString(aggregation.select), aggregation.type, actor, groupBy, m.getString(groupBy));
            }
        }

    }

    public void aggregateByGrouping(String id, String type_target, AggregationType type, Actor actor, String groupByColumn, String groupByValue) {
        Long target = null;
        try {
            target = Long.parseLong(type_target);
        } catch (NumberFormatException e) {
        }

        if (type == AggregationType.COUNT) {
            cacheAdapter.addGroupByItem(id, groupByColumn, groupByValue);
            return;
        } else if (type == AggregationType.COUNT_UNIQUE_X) {
            cacheAdapter.addToSet(id+":keys", groupByValue);
            cacheAdapter.incrementCounter(id + ":" + groupByValue);
        } else if(type == AggregationType.SELECT_UNIQUE_Xs) {
            if(type_target!=null)
                cacheAdapter.addToSet(id + ":" + groupByValue, type_target);
            cacheAdapter.addToSet(id+":keys", groupByValue);
        } else if (target == null)
            return;

        if (type == AggregationType.COUNT_X) {
            cacheAdapter.addGroupByItem(id, groupByColumn, groupByValue);
        } else if (type == AggregationType.SUM_X || type == AggregationType.AVERAGE_X) {
            cacheAdapter.addGroupByItem(id, groupByColumn, groupByValue, target);
        } else if (type == AggregationType.MINIMUM_X || type == AggregationType.MAXIMUM_X) {
            Long key = cacheAdapter.getCounter(id + ":" + groupByValue + ":" + target);
            if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                cacheAdapter.setCounter(id, target);
            cacheAdapter.addGroupByItem(id, groupByColumn, groupByValue);
        }
    }

    public void aggregateByNonGrouping(String id, String type_target, AggregationType type, Actor actor) {
        if (type == AggregationType.COUNT) {
            cacheAdapter.incrementCounter(id);
        } else if (type == AggregationType.COUNT_X) {
            if (type_target != null)
                cacheAdapter.incrementCounter(id);
        }
        if (type == AggregationType.SUM_X || type == AggregationType.AVERAGE_X) {
            Long target = null;
            try { target = Long.parseLong(type_target);} catch (NumberFormatException e) {}

            if (target != null) {
                cacheAdapter.incrementCounter(id, target);
            }
        } else if (type == AggregationType.MINIMUM_X || type == AggregationType.MAXIMUM_X) {
            Long target = null;
            try { target = Long.parseLong(type_target);} catch (NumberFormatException e) {}

            if (target != null) {
                Long key = cacheAdapter.getCounter(id + ":" + target);
                if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                    cacheAdapter.setCounter(id, target);
            }
        } else if (type == AggregationType.COUNT_UNIQUE_X || type == AggregationType.SELECT_UNIQUE_Xs) {
            if (type_target != null) {
                cacheAdapter.addToSet(id, type_target);
            }
        }
    }


}