package org.rakam.plugin;

import org.rakam.collection.Event;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SyncEventStore
        extends EventStore
{
    default CompletableFuture<Void> storeAsync(Event event) {
        store(event);
        return COMPLETED_FUTURE;
    }

    default CompletableFuture<int[]> storeBatchAsync(List<Event> events) {
        int[] ints = storeBatch(events);
        if(ints.length == 0) {
            return COMPLETED_FUTURE_BATCH;
        } else {
            return CompletableFuture.completedFuture(ints);
        }
    }

    void store(Event event);
    int[] storeBatch(List<Event> events);
}
