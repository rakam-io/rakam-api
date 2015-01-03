package org.rakam.database;

import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.model.Event;
import org.rakam.util.json.JsonObject;

import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public interface EventDatabase {
    void setupDatabase();

    void flushDatabase();

    void addEvent(String project, String eventName, String actor_id, JsonObject data);

    Event getEvent(UUID eventId);

    public void processRule(AnalysisRule rule);

    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn);
}
