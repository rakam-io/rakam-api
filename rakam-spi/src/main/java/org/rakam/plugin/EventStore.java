package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.Event;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface EventStore {
    int[] SUCCESSFUL_BATCH = new int[0];
    CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);
    CompletableFuture<int[]> COMPLETED_FUTURE_BATCH = CompletableFuture.completedFuture(new int[]{});

    default void store(Event event) {
        storeAsync(event).join();
    }

    default int[] storeBatch(List<Event> events) {
        return storeBatchAsync(events).join();
    }

    CompletableFuture<int[]> storeBatchAsync(List<Event> events);

    CompletableFuture<Void> storeAsync(Event event);

    default void storeBulk(List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        storeBatch(events);
    }

    enum CopyType {
        AVRO, CSV, JSON;

        @JsonCreator
        public static CopyType get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }

    enum CompressionType {
        GZIP;

        @JsonCreator
        public static CompressionType get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }
}
