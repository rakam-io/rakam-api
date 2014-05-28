package org.rakam.collection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.inject.Binding;
import com.google.inject.TypeLiteral;
import org.apache.log4j.Logger;
import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.cache.local.LocalCacheAdapter;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.plugin.CollectionMapperPlugin;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by buremba on 21/05/14.
 */
public class CollectionWorker extends Verticle implements Handler<Message<byte[]>> {
    final ExecutorService executor = new ThreadPoolExecutor(5, 20, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    final public static CacheAdapter localCacheAdapter = new LocalCacheAdapter();
    private CacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);
    private DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);
    Kryo kryo = new Kryo();
    static Logger logger = Logger.getLogger(CollectionWorker.class.getName());
    List<CollectionMapperPlugin> mappers;

    public void start() {
        vertx.eventBus().registerLocalHandler("collectionRequest", this);
        kryo.register(JsonObject.class);
        mappers = new ArrayList();
        for(Binding<CollectionMapperPlugin> mapper : ServiceStarter.injector.findBindingsByType(new TypeLiteral<CollectionMapperPlugin>() {})) {
            mappers.add(mapper.getProvider().get());
        }

    }

    @Override
    public void handle(final Message<byte[]> data) {
        Input in = new Input(data.body());
        final JsonObject message = kryo.readObject(in, JsonObject.class);

        for(CollectionMapperPlugin mapper : mappers) {
            if(!mapper.map(message))
                data.reply("0".getBytes());
        }

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try{
                    data.reply(process(message));
                } catch (Exception e) {
                    logger.error("error while processing collection request", e);
                }
            }
        });
    }

    public byte[] process(final JsonObject message) {
        String project = message.getString("_tracker");
        String actor_id = message.getString("_user");

        Iterator<String> it = message.getFieldNames().iterator();
        while(it.hasNext()) {
            String key = it.next();
            if (key.startsWith("_"))
                it.remove();
        }

         /*
            The current implementation change cabins monthly.
            However the interval must be determined by the system by taking account of data frequency
         */
        int time_cabin = Calendar.getInstance().get(Calendar.MONTH);

        /*ByteArrayOutputStream by = new ByteArrayOutputStream();
        Output out = new Output(by, 150);
        kryo.writeObject(out, message);*/
        databaseAdapter.addEvent(project, time_cabin, actor_id, message.encode().getBytes());

        aggregate(project, message, actor_id);
        return "1".getBytes();
    }

    /*
        Find pre-aggregation rules and match with the event.
        If it matches update the appropriate counter.
    */
    void aggregate(String project, JsonObject m, String actor_id) {

        AnalysisRuleList aggregations = DistributedAnalysisRuleMap.get(project);

        if (aggregations == null)
            return;

        JsonObject actor_props = null;
        for (AnalysisRule analysis_rule : aggregations) {
            if (analysis_rule.analysisType() == Analysis.ANALYSIS_METRIC || analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                AggregationRule aggregation = (AggregationRule) analysis_rule;

                FilterScript filters = aggregation.filters;
                if((filters!=null && filters.requiresUser()) || (aggregation.select!=null) && aggregation.select.requiresUser() || (aggregation.groupBy!=null) && aggregation.groupBy.requiresUser())
                    actor_props = get_actor_properties(project, actor_id);

                if (filters != null && !filters.test(m, actor_props))
                    continue;
                FieldScript groupBy = aggregation.groupBy;
                String key = analysis_rule.id;
                if (analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES)
                    key += ":" + ((TimeSeriesAggregationRule) analysis_rule).interval.spanCurrent().current();

                if (groupBy == null) {
                    aggregateByNonGrouping(key, aggregation.select == null ? null : aggregation.select.extract(m, actor_props), aggregation.type);
                } else {
                    String item = groupBy.extract(m, actor_props);
                    if (item != null)
                        aggregateByGrouping(key, aggregation.select == null ? null : aggregation.select.extract(m, actor_props), aggregation.type, groupBy.fieldKey, item);
                }
            }
        }
    }

    JsonObject get_actor_properties(String project, String actor_id) {
        if (actor_id != null) {
            JsonObject actor = cacheAdapter.getActorProperties(project, actor_id);
            if (actor == null) {
                Actor act = databaseAdapter.getActor(project, actor_id);
                if(act!=null) {
                    actor = act.data;
                    if (actor == null) {
                        actor = databaseAdapter.createActor(project, actor_id, null).data;
                    }
                    cacheAdapter.setActorProperties(project, actor_id, actor);
                }
            }
            return actor;
        }else
            return null;
    }

    public void aggregateByGrouping(String id, String type_target, AggregationType type, String groupByColumn, String groupByValue) {
        Long target = null;
        try {
            target = Long.parseLong(type_target);
        } catch (NumberFormatException e) {
        }

        if (type == AggregationType.COUNT) {
            localCacheAdapter.addGroupByItem(id, groupByColumn, groupByValue);
            return;
        } else if (type == AggregationType.SELECT_UNIQUE_X || type == AggregationType.COUNT_UNIQUE_X) {
            localCacheAdapter.addSet(id + ":" + (type_target != null ? type_target : "null"), groupByValue);
            localCacheAdapter.addSet(id + "::keys", type_target);
        } else if (target == null)
            return;

        if (type == AggregationType.COUNT_X) {
            localCacheAdapter.addGroupByItem(id, groupByColumn, groupByValue);
        } else if (type == AggregationType.SUM_X) {
            localCacheAdapter.addGroupByItem(id, groupByColumn, groupByValue, target);
        } else if (type == AggregationType.MINIMUM_X || type == AggregationType.MAXIMUM_X) {
            Long key = localCacheAdapter.getCounter(id + ":" + groupByValue + ":" + target);
            if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                localCacheAdapter.setCounter(id, target);
            localCacheAdapter.addGroupByItem(id, groupByColumn, groupByValue);
        }
    }

    public void aggregateByNonGrouping(String id, String type_target, AggregationType type) {
        if (type == AggregationType.COUNT) {
            localCacheAdapter.incrementCounter(id);
        } else if (type == AggregationType.COUNT_X) {
            if (type_target != null)
                localCacheAdapter.incrementCounter(id);
        }
        if (type == AggregationType.SUM_X) {
            Long target = null;
            try {
                target = Long.parseLong(type_target);
            } catch (NumberFormatException e) {
            }

            if (target != null)
                localCacheAdapter.incrementCounter(id, target);

        } else if (type == AggregationType.MINIMUM_X || type == AggregationType.MAXIMUM_X) {
            Long target = null;
            try {
                target = Long.parseLong(type_target);
            } catch (NumberFormatException e) {
            }

            if (target != null) {
                Long key = localCacheAdapter.getCounter(id + ":" + target);
                if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                    localCacheAdapter.setCounter(id, target);
            }
        } else if (type == AggregationType.COUNT_UNIQUE_X || type == AggregationType.SELECT_UNIQUE_X) {
            if (type_target != null)
                localCacheAdapter.addSet(id, type_target);
        }
    }
}

