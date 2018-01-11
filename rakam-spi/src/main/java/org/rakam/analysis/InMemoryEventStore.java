package org.rakam.analysis;

import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;

import java.util.ArrayList;
import java.util.List;

public class InMemoryEventStore implements SyncEventStore {
    private final List<Event> events = new ArrayList<>();

    @Override
    public synchronized void store(Event event) {
        events.add(event);
    }

    @Override
    public int[] storeBatch(List<Event> events) {
        this.events.addAll(events);
        return EventStore.SUCCESSFUL_BATCH;
    }

    public List<Event> getEvents() {
        return events;
    }
}
