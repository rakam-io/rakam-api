package org.rakam.database;

import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.model.Actor;
import org.rakam.model.Event;

import java.util.*;

/**
 * Created by buremba on 21/12/13.
 */
public interface DatabaseAdapter {
    void setupDatabase();
    void flushDatabase();

    Actor createActor(String project, String actor_id, Map<String, String> properties);
    void addPropertyToActor(String project, String actor_id, Map<String, String> props);
    UUID addEvent(String project, int time_cabin, String actor_id, byte[] data);
    void addEventAsync(String project, int time_cabin, String actor_id, byte[] data);
    Actor getActor(String project, String actorId);
    Event getEvent(UUID eventId);
    void combineActors(String actor1, String actor2);

    void addSet(String key, String item);
    void addSet(String key, Set<String> items);
    Set<String> getSet(String key);
    int getSetCount(String key);
    Iterator<String> getSetIterator(String key);

    void incrementCounter(String key);
    long getCounter(String key);
    void incrementCounter(String key, long incrementBy);
    Map<String, Long> getCounters(Collection<String> keys);

    Map<String, AnalysisRuleList> getAllRules();
    void addRule(AnalysisRule rule);
    void deleteRule(AnalysisRule rule);
}
