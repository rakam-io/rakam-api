package org.rakam.plugin;

import org.rakam.collection.Event;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 03:25.
 */
public interface EventStore {
    public void store(Event event);
}
