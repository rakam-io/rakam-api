package org.rakam.collection;

import com.esotericsoftware.kryo.Kryo;
import com.hazelcast.core.IMap;
import org.rakam.analysis.rule.AnalysisRule;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.cache.CacheAdapterFactory;
import org.rakam.cache.SimpleCacheAdapter;
import org.rakam.cache.local.LocalCacheAdapter;
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
import java.util.Map;
import java.util.UUID;

public class CollectionRequestHandler extends Verticle implements Handler<Message<JsonObject>> {
    private EventBus eb;
    private String address = UUID.randomUUID().toString();
    private IMap<String, AnalysisRuleList> preAggregation;
    final public static SimpleCacheAdapter cacheAdapter = new LocalCacheAdapter();;
    private DatabaseAdapter databaseAdapter;
    Kryo kryo = new Kryo();

    @Override
    public void start() {
        eb = vertx.eventBus();
        kryo.register(JsonObject.class);
        eb.registerHandler(address, this);
        JsonObject msg = new JsonObject().putString("processor", address);
        eb.send("request.orderQueue.register", msg);
        databaseAdapter = new CassandraAdapter();
        preAggregation = CacheAdapterFactory.getAggregationMap();
        JsonObject config = container.config();
        long timerID = vertx.setPeriodic(1000, new Handler<Long>() {
            public void handle(Long timerID) {
                for(Map.Entry item : preAggregation.entrySet())
                    PeriodicCollector.process(item);
            }
        });

    }


    @Override
    public void stop() {
        JsonObject msg = new JsonObject().putString("processor", address);
        eb.send("request.orderQueue.unregister", msg);
        eb.unregisterHandler(address, this);
    }

    public void handle(Message<JsonObject> m) {

        JsonObject message = m.body();
        String project = message.getString("_tracker");
        String actor_id = message.getString("_user");


        /*
        JsonObject data = new JsonObject();
        for (Map.Entry<String, String> item : message) {
            String key = item.getKey();
            if (!key.startsWith("_"))
                event.putString(item.getKey(), item.getValue());
        }
        */


         /*
            The current implementation change cabins monthly.
            However the interval must be determined by the system by taking account of data frequency
         */
        int time_cabin = Calendar.getInstance().get(Calendar.MONTH);
        databaseAdapter.addEventAsync(project, time_cabin, actor_id, message.encode().getBytes());

        Actor actor = null;
        if (actor_id != null) {
            JsonObject actor_cache_props = cacheAdapter.getActorProperties(project, actor_id);
            if (actor_cache_props == null) {
                actor = databaseAdapter.getActor(project, actor_id);
                if (actor == null) {
                    actor = databaseAdapter.createActor(project, actor_id, null);
                }
                cacheAdapter.setActorProperties(project, actor_id, actor.data);
            } else {
                actor = new Actor(project, actor_id, actor_cache_props);
            }
        }

        aggregate(project, message, actor);
        JsonObject ret = new JsonObject();
        m.reply(ret);
    }

    /*
        Find pre-aggregation rules and match with the event.
        If it matches update the appropriate counter.
    */
    public void aggregate(String project, JsonObject m, Actor actor) {

        AnalysisRuleList aggregations = preAggregation.get(project);

        if (aggregations == null)
            return;

        for (AnalysisRule analysis_rule : aggregations) {
            if (analysis_rule.analysisType() == Analysis.ANALYSIS_METRIC || analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                AggregationRule aggregation = (AggregationRule) analysis_rule;
                // this is not the best approach but enough for demo
                if(actor!=null && actor.data!=null)
                    for(Map.Entry<String, Object> entry : actor.data.toMap().entrySet())
                        m.putValue("_user." + entry.getKey(), entry.getValue());

                FilterScript filters = aggregation.filters;
                if (filters != null && !filters.test(m))
                    continue;
                FieldScript groupBy = aggregation.groupBy;
                String key = analysis_rule.id;
                if (analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES)
                    key += ":" + ((TimeSeriesAggregationRule) analysis_rule).interval.spanCurrent().current();

                if (groupBy == null) {
                    aggregateByNonGrouping(key, aggregation.select==null ? null : aggregation.select.extract(m), aggregation.type, actor);
                } else {
                    String item = groupBy.extract(m);
                    if (item != null)
                        aggregateByGrouping(key, aggregation.select==null ? null : aggregation.select.extract(m), aggregation.type, actor, groupBy.fieldKey, item);
                }
            }
        }
    }

    public void aggregateByGrouping(String id, String type_target, AggregationType type, Actor actor, String groupByColumn, String groupByValue) {
        Long target = null;
        try {
            target = Long.parseLong(type_target);
        } catch (NumberFormatException e) {}

        if (type == AggregationType.COUNT) {
            cacheAdapter.addGroupByItem(id, groupByColumn, groupByValue);
            return;
        } else if (type == AggregationType.COUNT_UNIQUE_X) {
            cacheAdapter.addToSet(id + ":keys", groupByValue);
            cacheAdapter.incrementCounter(id + ":" + groupByValue);
        } else if (type == AggregationType.SELECT_UNIQUE_Xs) {
            if (type_target != null)
                cacheAdapter.addToSet(id + ":" + groupByValue, type_target);
            cacheAdapter.addToSet(id + ":keys", groupByValue);
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
            try {
                target = Long.parseLong(type_target);
            } catch (NumberFormatException e) {}

            if (target != null)
                cacheAdapter.incrementCounter(id, target);

        } else if (type == AggregationType.MINIMUM_X || type == AggregationType.MAXIMUM_X) {
            Long target = null;
            try {
                target = Long.parseLong(type_target);
            } catch (NumberFormatException e) {
            }

            if (target != null) {
                Long key = cacheAdapter.getCounter(id + ":" + target);
                if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                    cacheAdapter.setCounter(id, target);
            }
        } else if (type == AggregationType.COUNT_UNIQUE_X || type == AggregationType.SELECT_UNIQUE_Xs) {
            if (type_target != null)
                cacheAdapter.addToSet(id, type_target);
        }
    }
}