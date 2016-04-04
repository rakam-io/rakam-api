package org.rakam.plugin;

import org.rakam.collection.Event;

import java.util.List;


public interface EventStore {
    int[] SUCCESSFUL_BATCH = new int[0];

    void store(Event event);

    int[] storeBatch(List<Event> events);

    default void storeBulk(List<Event> events, boolean commit) {
        throw new UnsupportedOperationException();
    }

    default void commit(String project, String collection) {
        throw new UnsupportedOperationException();
    }
}
