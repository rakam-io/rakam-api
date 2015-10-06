package org.rakam.plugin;

import org.rakam.collection.Event;


public interface EventStore {
    void store(Event event);
}
