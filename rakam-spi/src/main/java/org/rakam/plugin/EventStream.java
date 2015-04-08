package org.rakam.plugin;


import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:06.
 */
public interface EventStream {
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response);
    public static interface EventStreamer {
        public void sync();
        public void shutdown();
    }
}
