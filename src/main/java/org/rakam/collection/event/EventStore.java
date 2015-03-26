package org.rakam.collection.event;

import org.rakam.report.Event;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 03:25.
 */
public interface EventStore {
    public void store(Event event);
}
