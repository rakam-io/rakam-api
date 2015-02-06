package org.rakam.database;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AggregationReport;
import org.rakam.model.Event;

import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public interface EventDatabase {
    void setupDatabase();

    void flushDatabase();

    void addEvent(String project, String eventName, String actor_id, ObjectNode data);

    Event getEvent(UUID eventId);

    public void processRule(AggregationReport rule);

    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn);
}
