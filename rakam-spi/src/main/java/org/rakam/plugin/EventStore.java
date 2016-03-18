package org.rakam.plugin;

import org.rakam.collection.Event;

import java.util.List;


public interface EventStore {
    int[] SUCCESSFUL_BATCH = new int[0];

    void store(Event event);

    int[] storeBatch(List<Event> events);
}
