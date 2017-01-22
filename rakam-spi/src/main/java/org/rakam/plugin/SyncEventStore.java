package org.rakam.plugin;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.rakam.collection.Event;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SyncEventStore
        extends EventStore
{
    EventLoopGroup workerGroup = new NioEventLoopGroup();

    default CompletableFuture<Void> storeAsync(Event event) {
        return CompletableFuture.supplyAsync(() -> {
            store(event);
            return null;
        }, workerGroup);
    }

    default CompletableFuture<int[]> storeBatchAsync(List<Event> events) {
        return CompletableFuture.supplyAsync(() -> storeBatch(events), workerGroup);


    }

    void store(Event event);
    int[] storeBatch(List<Event> events);
}
