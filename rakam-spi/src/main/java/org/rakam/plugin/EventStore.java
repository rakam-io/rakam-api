package org.rakam.plugin;

import org.rakam.collection.Event;


public interface EventStore {
    void store(Event event);
    default void storeBatch(Event[] events) {
        for (Event event : events) {
            store(event);
        }
    }
}
