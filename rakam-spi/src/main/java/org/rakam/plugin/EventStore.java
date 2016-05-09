package org.rakam.plugin;

import org.rakam.collection.Event;
import org.rakam.report.QueryExecution;

import java.util.List;


public interface EventStore {
    int[] SUCCESSFUL_BATCH = new int[0];

    void store(Event event);

    int[] storeBatch(List<Event> events);

    default QueryExecution storeBulk(List<Event> events, boolean commit) {
        throw new UnsupportedOperationException();
    }

    default QueryExecution commit(String project, String collection) {
        throw new UnsupportedOperationException();
    }
}
