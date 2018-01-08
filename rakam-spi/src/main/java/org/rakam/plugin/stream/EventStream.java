package org.rakam.plugin.stream;


import java.util.List;


public interface EventStream {
    EventStreamer subscribe(String project,
                            List<CollectionStreamQuery> collections,
                            List<String> columns,
                            StreamResponse response);

    interface EventStreamer {
        void sync();

        void shutdown();
    }
}
