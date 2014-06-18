package org.rakam.collection;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.analysis.script.FieldScript;
import org.rakam.analysis.script.FilterScript;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.KeyValueStorage;
import org.rakam.model.Actor;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by buremba on 05/06/14.
 */
public class Aggregator {
    final private CacheAdapter cacheAdapter;
    private DatabaseAdapter databaseAdapter;
    private final static ConcurrentMap<String, JsonObject> lruCache = new ConcurrentLinkedHashMap.Builder()
            .maximumWeightedCapacity(10000)
            .build();

    final private KeyValueStorage storageAdapter;

    public Aggregator(KeyValueStorage storage, CacheAdapter cache, DatabaseAdapter database) {
        storageAdapter = storage;
        cacheAdapter = cache;
        databaseAdapter = database;
    }

    /*
    Find pre-aggregation rules and match with the event.
    If it matches update the appropriate counter.
*/
    public void aggregate(String project, JsonObject m, String actor_id, int timestamp) {

        AnalysisRuleList aggregations = DistributedAnalysisRuleMap.get(project);

        if (aggregations == null)
            return;

        JsonObject actor_props = null;
        for (AnalysisRule analysis_rule : aggregations) {
            if (analysis_rule.analysisType() == Analysis.ANALYSIS_METRIC || analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                AggregationRule aggregation = (AggregationRule) analysis_rule;

                FilterScript filters = aggregation.filters;
                if ((filters != null && filters.requiresUser()) || (aggregation.select != null) && aggregation.select.requiresUser() || (aggregation.groupBy != null) && aggregation.groupBy.requiresUser())
                    actor_props = get_actor_properties(project, actor_id);

                if (filters != null && !filters.test(m, actor_props))
                    continue;
                FieldScript groupBy = aggregation.groupBy;
                String key = analysis_rule.id;
                if (analysis_rule.analysisType() == Analysis.ANALYSIS_TIMESERIES)
                    key += ":" + ((TimeSeriesAggregationRule) analysis_rule).interval.span(timestamp).current();

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

    public void aggregateByGrouping(String id, String type_target, AggregationType type, String groupByColumn, String groupByValue) {

        switch (type) {
            case COUNT:
                storageAdapter.addGroupByItem(id, groupByColumn, groupByValue);
                break;
            case SELECT_UNIQUE_X:
            case COUNT_UNIQUE_X:
                storageAdapter.addSet(id + ":" + groupByValue, (type_target != null ? type_target : "null"));
                storageAdapter.addSet(id + "::keys", groupByValue);
                break;
            case COUNT_X:
                storageAdapter.addGroupByItem(id, groupByColumn, groupByValue);
            default:
                Long target = null;
                try {
                    target = Long.parseLong(type_target, 10);
                } catch (NumberFormatException e) {}

                if (type == AggregationType.SUM_X) {
                    storageAdapter.addGroupByItem(id, groupByColumn, groupByValue, target);
                } else if (type == AggregationType.MINIMUM_X || type == AggregationType.MAXIMUM_X) {
                    Long key = storageAdapter.getCounter(id + ":" + groupByValue + ":" + target);
                    if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                        storageAdapter.setCounter(id, target);
                    storageAdapter.addGroupByItem(id, groupByColumn, groupByValue);
                }
        }

    }

    public void aggregateByNonGrouping(String id, String type_target, AggregationType type) {
        if (type == AggregationType.COUNT) {
            storageAdapter.incrementCounter(id);
        } else if (type == AggregationType.COUNT_X) {
            if (type_target != null)
                storageAdapter.incrementCounter(id);
        }
        if (type == AggregationType.SUM_X) {
            Long target = null;
            try {
                target = Long.parseLong(type_target);
            } catch (NumberFormatException e) {
            }

            if (target != null)
                storageAdapter.incrementCounter(id, target);

        } else if (type == AggregationType.MINIMUM_X || type == AggregationType.MAXIMUM_X) {
            Long target;
            try {
                target = Long.parseLong(type_target);
            } catch (NumberFormatException e) {
                return;
            }

            Long key = storageAdapter.getCounter(id + ":" + target);
            if (type == AggregationType.MAXIMUM_X ? target > key : target < key)
                storageAdapter.setCounter(id, target);

        } else if (type == AggregationType.COUNT_UNIQUE_X || type == AggregationType.SELECT_UNIQUE_X) {
            if (type_target != null)
                storageAdapter.addSet(id, type_target);
        }
    }

    JsonObject get_actor_properties(String project, String actor_id) {
        if (actor_id != null) {
            JsonObject lru_actor = lruCache.get(project + ":" + actor_id);
            if (lru_actor != null)
                return lru_actor;

            JsonObject actor = cacheAdapter.getActorProperties(project, actor_id);
            if (actor == null) {
                Actor act = databaseAdapter.getActor(project, actor_id);
                if (act != null) {
                    actor = act.data;
                    cacheAdapter.setActorProperties(project, actor_id, actor);
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
