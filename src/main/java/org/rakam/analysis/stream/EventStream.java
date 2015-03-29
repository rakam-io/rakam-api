package org.rakam.analysis.stream;

import org.rakam.server.http.RakamHttpRequest;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:06.
 */
public interface EventStream {
    public EventStreamer subscribe(String project, List<StreamHttpService.CollectionStreamQuery> collections, List<String> columns, RakamHttpRequest.StreamResponse response);
    public static interface EventStreamer {
        public void sync();
    }
}
