package org.rakam.database;

import org.rakam.model.Actor;
import org.rakam.model.Event;

import java.util.*;

/**
 * Created by buremba on 21/12/13.
 */
public interface DatabaseAdapter {
    void setupDatabase();
    void destroy();
    Actor createActor(String project, String actor_id, Map<String, String> properties);
    void addPropertyToActor(String project, String actor_id, Map<String, String> props);
    UUID addEvent(String project, int time_cabin, String actor_id, byte[] data);
    void addEventAsync(String project, int time_cabin, String actor_id, byte[] data);
    Actor getActor(String project, String actorId);
    Event getEvent(UUID eventId);
    void combineActors(String actor1, String actor2);
    void flushDatabase();

    void addSet(String key, String item);
    void addSet(String key, Set<String> items);
    void incrementCounter(String key);
    long getCounter(String key);
    Set<String> getSet(String key);
    void incrementCounter(String key, long incrementBy);

    int getSetCount(String key);

    Iterator<String> getSetIterator(String key);

    Map<String, Long> getMultiCounts(Collection<String> keys);
}
