package org.rakam.database;

import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.model.Actor;
import org.rakam.model.Event;
import org.vertx.java.core.json.JsonObject;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public interface DatabaseAdapter extends CacheAdapter {
    void setupDatabase();
    void flushDatabase();

    Actor createActor(String project, String actor_id, Map<String, Object> properties);
    void addPropertyToActor(String project, String actor_id, Map<String, Object> props);
    void addEvent(String project, String event_id, JsonObject data);
    Actor getActor(String project, String actorId);
    Event getEvent(UUID eventId);
    void combineActors(String actor1, String actor2);
    int getSetCount(String key);
    Map<String, Long> getCounters(Collection<String> keys);
    public void processRule(AnalysisRule rule);
    public void processRule(AnalysisRule rule, long start_time, long end_time);
    public void batch(String project, int start_time, int end_time, int nodeId);
    public void batch(String project, int start_time, int nodeId);
    public Actor[] filterActors(FilterScript filter, int limit, String orderByColumn);
    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn);
}
