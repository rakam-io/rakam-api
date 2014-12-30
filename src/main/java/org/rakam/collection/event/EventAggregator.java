package org.rakam.collection.event;

import com.google.inject.Injector;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.rakam.analysis.AnalysisRuleMap;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.stream.StreamAdapter;
import org.rakam.util.TimeUtil;
import org.rakam.util.json.JsonObject;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by buremba on 05/06/14.
 */
public class EventAggregator {
    private final AnalysisRuleMap rules;
    private final DatabaseAdapter databaseAdapter;
    private final StreamAdapter streamAdapter;
    private ActorCacheAdapter actorCache;
    private static ConcurrentMap<String, JsonObject> lruCache;

    public EventAggregator(Injector injector, AnalysisRuleMap rules) {
        this.rules = rules;
        actorCache = injector.getInstance(ActorCacheAdapter.class);
        databaseAdapter = injector.getInstance(DatabaseAdapter.class);
        streamAdapter = injector.getInstance(StreamAdapter.class);
    }

    public void activeActorCache() {
        lruCache = new ConcurrentLinkedHashMap.Builder()
                .maximumWeightedCapacity(10000)
                .build();
    }

    /*
    Find pre-aggregation rules and match with the event.
    If it matches update the appropriate counter.
    */
    public void aggregate(String project, JsonObject m, String actor_id, Integer timestamp) {
        if (timestamp == null) {
            timestamp = TimeUtil.UTCTime();
        }

        Set<AnalysisRule> aggregations = rules.get(project);
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

                Object type_target = aggregation.select == null ? null : aggregation.select.extract(m, actor_props);

                if (aggregation.groupBy != null) {
                    String groupByValue;
                    if (aggregation.groupBy != null) {
                        Object extract = aggregation.groupBy.extract(m, actor_props);
                        groupByValue = extract != null ? extract.toString() : null;
                    } else {
                        groupByValue = null;
                    }
                    if(rule instanceof TimeSeriesAggregationRule) {
                        streamAdapter.handleGroupingTimeSeries(key, aggregation.type, type_target, groupByValue, ((TimeSeriesAggregationRule) rule).interval.span(timestamp).current());
                    }else {
                        streamAdapter.handleGroupingMetric(key, aggregation.type, type_target, groupByValue);
                    }
                } else {
                    if(rule instanceof TimeSeriesAggregationRule) {
                        streamAdapter.handleNonGroupingTimeSeries(key, aggregation.type, type_target, ((TimeSeriesAggregationRule) rule).interval.span(timestamp).current());
                    }else {
                        streamAdapter.handleNonGroupingMetric(key, aggregation.type, type_target);
                    }
                }
            }
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
                actor = actorCache.getActorProperties(project, actor_id).join();
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
