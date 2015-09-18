package org.rakam.plugin;


import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:06.
 */
public interface EventStream {
    EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response);
    interface EventStreamer {
        void sync();
        void shutdown();
    }
}
