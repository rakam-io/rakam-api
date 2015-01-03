package org.rakam.collection.event;

import com.google.inject.Injector;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.constant.Analysis;
import org.rakam.database.ActorDatabase;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.model.Actor;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.util.TimeUtil;
import org.rakam.util.json.JsonObject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by buremba on 05/06/14.
 */
public class EventAggregator {
    private final AnalysisRuleDatabase rules;
    private final EventDatabase databaseAdapter;
    private final ActorDatabase actorDatabaseAdapter;
    private ActorCacheAdapter actorCache;
    private static ConcurrentMap<String, JsonObject> lruCache;

    public EventAggregator(Injector injector, AnalysisRuleDatabase rules) {
        this.rules = rules;
        actorCache = injector.getInstance(ActorCacheAdapter.class);
        databaseAdapter = injector.getInstance(EventDatabase.class);
        actorDatabaseAdapter = injector.getInstance(ActorDatabase.class);
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
                        for (Map.Entry<String, Object> s : actor_props) {
                            m.put("_user." + s, s.getValue());
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
                    //
                } else {
                    //
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
                Actor act = actorDatabaseAdapter.getActor(project, actor_id);
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
                    actor = actorDatabaseAdapter.createActor(project, actor_id, null).data;
                }
            }
            return actor;
        } else
            return null;
    }
}
