package org.rakam.collection;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.ActorCacheAdapter;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.util.ConversionUtil;
import org.vertx.java.core.json.JsonObject;

import java.util.HashSet;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by buremba on 05/06/14.
 */
public class EventAggregator {
    final private CacheAdapter l1cacheAdapter;
    final private CacheAdapter l2cacheAdapter;
    private ActorCacheAdapter actorCache;
    private DatabaseAdapter databaseAdapter;
    private final static ConcurrentMap<String, JsonObject> lruCache = new ConcurrentLinkedHashMap.Builder()
            .maximumWeightedCapacity(10000)
            .build();

    public EventAggregator(CacheAdapter l1Cache, CacheAdapter l2Cache, DatabaseAdapter database) {
        l1cacheAdapter = l1Cache;
        l2cacheAdapter = l2Cache;
        actorCache = (ActorCacheAdapter) l2cacheAdapter;
        databaseAdapter = database;
    }
    public EventAggregator(CacheAdapter l1Cache, CacheAdapter l2Cache, DatabaseAdapter database, ActorCacheAdapter actor) {
        l1cacheAdapter = l1Cache;
        l2cacheAdapter = l2Cache;
        databaseAdapter = database;
        actorCache = actor;
    }

    /*
    Find pre-aggregation rules and match with the event.
    If it matches update the appropriate counter.
    */
    public void aggregate(String project, JsonObject m, String actor_id, int timestamp) {

        HashSet<AnalysisRule> aggregations = DistributedAnalysisRuleMap.get(project);

        if (aggregations == null)
            return;

        JsonObject actor_props = null;

        for (AnalysisRule analysis_rule : aggregations) {
            if (analysis_rule.analysisType() == Analysis.ANALYSIS_METRIC || analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                AggregationRule aggregation = (AggregationRule) analysis_rule;

                FilterScript filters = aggregation.filters;
                if ((filters != null && filters.requiresUser()) ||
                        ((aggregation.select != null) && aggregation.select.requiresUser()) ||
                        ((aggregation.groupBy != null) && aggregation.groupBy.requiresUser())) {
                    JsonObject actor_properties = get_actor_properties(project, actor_id);
                    for (String s : actor_properties.getFieldNames()) {
                        m.putValue("_user."+s, actor_properties.getValue(s));
                    }
                }

                if (filters != null && !filters.test(m))
                    continue;


                String key = analysis_rule.id();
                if (analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES)
                    key += ":" + ((TimeSeriesAggregationRule) analysis_rule).interval.span(timestamp).current();

                if (aggregation.groupBy!=null) {
                    String groupByValue = aggregation.groupBy!=null ? aggregation.groupBy.extract(m, actor_props) : null;
                    aggregateByGrouping(key, aggregation.select == null ? null : aggregation.select.extract(m, actor_props), aggregation.type, groupByValue);
                }else {
                    aggregateByNonGrouping(key, aggregation.select == null ? null : aggregation.select.extract(m, actor_props), aggregation.type);
                }
            }
        }
    }

    public void aggregateByGrouping(String id, String type_target, AggregationType type, String groupByValue) {
        groupByValue = groupByValue == null ? "null" : groupByValue;
        type_target = type_target==null ? "null" : type_target;
        switch (type) {
            case COUNT:
                l1cacheAdapter.addGroupByCounter(id, groupByValue);
                break;
            case UNIQUE_X:
                l1cacheAdapter.addGroupByString(id, groupByValue, type_target);
                break;
            case COUNT_X:
                if (groupByValue!=null)
                    l1cacheAdapter.addGroupByCounter(id, groupByValue);
            default:
                try {
                    Long target = ConversionUtil.toLong(type_target, 10L);
                    switch (type) {
                        case SUM_X:
                            l1cacheAdapter.addGroupByCounter(id, groupByValue, target);
                        case MINIMUM_X:
                        case MAXIMUM_X:
                            Long key = l1cacheAdapter.getCounter(id + ":" + groupByValue + ":" + target);
                            if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                                l1cacheAdapter.setCounter(id, target);
                            l1cacheAdapter.addGroupByCounter(id, groupByValue);
                        case AVERAGE_X:
                            l1cacheAdapter.incrementGroupByAverageCounter(id, groupByValue, target, 1);
                    }
                } catch (NumberFormatException e) {}
        }
    }

    public void aggregateByNonGrouping(String id, String type_target, AggregationType type) {

        switch (type) {
            case COUNT:
                l1cacheAdapter.incrementCounter(id);
                break;
            case COUNT_X:
                if (type_target != null)
                    l1cacheAdapter.incrementCounter(id);
                break;
            case SUM_X:
                try {
                    l1cacheAdapter.incrementCounter(id, ConversionUtil.toLong(type_target));
                } catch (NumberFormatException e) {}
                break;
            case MINIMUM_X:
            case MAXIMUM_X:
                try {
                    Long target = ConversionUtil.toLong(type_target);
                    Long key = l1cacheAdapter.getCounter(id + ":" + target);
                    if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                        l1cacheAdapter.setCounter(id, target);
                } catch (NumberFormatException e) { }
                break;
            case UNIQUE_X:
                if (type_target != null)
                 l1cacheAdapter.addSet(id, type_target);
                break;
            case AVERAGE_X:
                try {
                    Long target = ConversionUtil.toLong(type_target);
                    l1cacheAdapter.incrementAverageCounter(id, target, 1);
                } catch (NumberFormatException e) { }
                break;

        }
    }

    JsonObject get_actor_properties(String project, String actor_id) {
        if (actor_id != null) {
            JsonObject lru_actor = lruCache.get(project + ":" + actor_id);
            if (lru_actor != null)
                return lru_actor;

            JsonObject actor = actorCache.getActorProperties(project, actor_id);
            if (actor == null) {
                Actor act = databaseAdapter.getActor(project, actor_id);
                if (act != null) {
                    actor = act.data;
                    actorCache.setActorProperties(project, actor_id, actor);
                    lruCache.put(project + ":" + actor_id, actor);
                } else {
                    actor = databaseAdapter.createActor(project, actor_id, null).data;
                }
            }
            return actor;
        } else
            return null;
    }
}
