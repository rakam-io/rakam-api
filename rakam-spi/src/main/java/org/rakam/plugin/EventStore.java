package org.rakam.plugin;

import org.rakam.collection.Event;


public interface EventStore {
    public void store(Event event);
}
