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
import org.rakam.util.SpanTime;
import org.vertx.java.core.json.JsonObject;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.rakam.util.DateUtil.UTCTime;

/**
 * Created by buremba on 05/06/14.
 */
public class EventAggregator {
    final private CacheAdapter l1cacheAdapter;
    final private CacheAdapter l2cacheAdapter;
    private ActorCacheAdapter actorCache;
    private DatabaseAdapter databaseAdapter;
    private static ConcurrentMap<String, JsonObject> lruCache;

    public EventAggregator(CacheAdapter l1Cache, CacheAdapter l2Cache, DatabaseAdapter database) {
        this(l1Cache, l2Cache, database, null);
    }

    public EventAggregator(CacheAdapter l1Cache, CacheAdapter l2Cache, DatabaseAdapter database, ActorCacheAdapter actor) {
        l1cacheAdapter = l1Cache;
        l2cacheAdapter = l2Cache;
        databaseAdapter = database;
        actorCache = actor;
    }

    public void activeActorCache() {
        lruCache = new ConcurrentLinkedHashMap.Builder()
                .maximumWeightedCapacity(10000)
                .build();
    }

    public void aggregate(String project, JsonObject m, String actor_id) {
        aggregate(project, m, actor_id, UTCTime());
    }

    /*
    Find pre-aggregation rules and match with the event.
    If it matches update the appropriate counter.
    */
    public void aggregate(String project, JsonObject m, String actor_id, int timestamp) {

        Set<AnalysisRule> aggregations = DistributedAnalysisRuleMap.get(project);
        if (aggregations == null)
            return;

        JsonObject actor_props = null;

        for (AnalysisRule rule : aggregations) {
            if (rule.analysisType() == Analysis.ANALYSIS_METRIC || rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                AggregationRule aggregation = (AggregationRule) rule;

                FilterScript filters = aggregation.filters;
                if ((filters != null && filters.requiresUser()) ||
                        ((aggregation.select != null) && aggregation.select.requiresUser()) ||
                        ((aggregation.groupBy != null) && aggregation.groupBy.requiresUser())) {
                    actor_props = get_actor_properties(project, actor_id);
                    if (actor_props != null) {
                        for (String s : actor_props.getFieldNames()) {
                            m.putValue("_user." + s, actor_props.getValue(s));
                        }
                    }
                }

                if (filters != null && !filters.test(m))
                    continue;

                String key = rule.id();
                CacheAdapter adapter;
                if (rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                    SpanTime span = new SpanTime(((TimeSeriesAggregationRule) rule).interval).span(timestamp);
                    key += ":" + span.current();
                    adapter = span.current() == span.spanCurrent().current() ? l1cacheAdapter : databaseAdapter;
                } else {
                    adapter = l1cacheAdapter;
                }

                if (aggregation.groupBy != null) {
                    String groupByValue = aggregation.groupBy != null ? aggregation.groupBy.extract(m, actor_props) : null;
                    Object type_target = aggregation.select == null ? null : aggregation.select.extract(m, actor_props);
                    aggregateByGrouping(adapter, key, type_target, aggregation.type, groupByValue);
                } else {
                    aggregateByNonGrouping(adapter, key, aggregation.select == null ? null : aggregation.select.extract(m, actor_props), aggregation.type);
                }
            }
        }
    }

    public void aggregateByGrouping(CacheAdapter adapter, String id, Object type_target, AggregationType type, String groupByValue) {
        groupByValue = groupByValue == null ? "null" : groupByValue;
        type_target = type_target == null ? "null" : type_target;
        switch (type) {
            case COUNT:
                adapter.incrementGroupBySimpleCounter(id, groupByValue, 1);
                break;
            case UNIQUE_X:
                adapter.addGroupByString(id, groupByValue, type_target.toString());
                break;
            case COUNT_X:
                if (groupByValue != null)
                    adapter.incrementGroupBySimpleCounter(id, groupByValue, 1);
            default:
                Long target = ConversionUtil.toLong(type_target);
                if (target != null) {
                    switch (type) {
                        case SUM_X:
                            adapter.incrementGroupBySimpleCounter(id, groupByValue, target);
                            break;
                        case MAXIMUM_X:
                            Long key = adapter.getCounter(id + ":" + groupByValue);
                            if (target > key)
                                adapter.setCounter(id, target);
                            adapter.incrementGroupBySimpleCounter(id, groupByValue, 1);
                            break;
                        case MINIMUM_X:
                            key = adapter.getCounter(id + ":" + groupByValue);
                            if (target < key)
                                adapter.setCounter(id, target);
                            adapter.incrementGroupBySimpleCounter(id, groupByValue, 1);
                            break;
                        case AVERAGE_X:
                            adapter.incrementGroupByAverageCounter(id, groupByValue, target, 1);
                            break;
                    }
                }
        }
    }

    public void aggregateByNonGrouping(CacheAdapter adapter, String id, Object type_target, AggregationType type) {

        switch (type) {
            case COUNT:
                adapter.incrementCounter(id);
                break;
            case COUNT_X:
                if (type_target != null)
                    adapter.incrementCounter(id);
                break;
            case SUM_X:
                try {
                    adapter.incrementCounter(id, ConversionUtil.toLong(type_target));
                } catch (NumberFormatException e) {
                }
                break;
            case MINIMUM_X:
            case MAXIMUM_X:
                try {
                    Long target = ConversionUtil.toLong(type_target);
                    Long key = adapter.getCounter(id);
                    if (key == null || (type == AggregationType.MAXIMUM_X ? target > key : target < key))
                        adapter.setCounter(id, target);
                } catch (NumberFormatException e) {
                }
                break;
            case UNIQUE_X:
                if (type_target != null)
                    adapter.addSet(id, type_target.toString());
                break;
            case AVERAGE_X:
                try {
                    Long target = ConversionUtil.toLong(type_target);
                    adapter.incrementAverageCounter(id, target, 1);
                } catch (NumberFormatException e) {
                }
                break;

        }
    }

    JsonObject get_actor_properties(String project, String actor_id) {
        if (actor_id != null) {
            if (lruCache != null) {
                JsonObject lru_actor = lruCache.get(project + ":" + actor_id);
                if (lru_actor != null)
                    return lru_actor;
            }

            JsonObject actor = null;
            if (actorCache != null) {
                actor = actorCache.getActorProperties(project, actor_id);
            }
            if (actor == null) {
                Actor act = databaseAdapter.getActor(project, actor_id);
                if (act != null) {
                    actor = act.data;
                    if (actorCache != null) {
                        actorCache.setActorProperties(project, actor_id, actor);
                    }
                    if (lruCache != null) {
                        lruCache.put(project + ":" + actor_id, actor);
                    }
                } else {
                    // todo: it should be optional
                    actor = databaseAdapter.createActor(project, actor_id, null).data;
                }
            }
            return actor;
        } else
            return null;
    }
}
