package org.rakam.plugin;

import org.rakam.collection.Event;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public interface SyncEventStore
        extends EventStore
{
    ThreadPoolExecutor workerGroup = new ThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors() * 6,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>());

    default CompletableFuture<Void> storeAsync(Event event)
    {
        return CompletableFuture.supplyAsync(() -> {
            store(event);
            return null;
        }, workerGroup);
    }

    default CompletableFuture<int[]> storeBatchAsync(List<Event> events)
    {
        return CompletableFuture.supplyAsync(() -> storeBatch(events), workerGroup);
    }

    void store(Event event);

    int[] storeBatch(List<Event> events);
}
