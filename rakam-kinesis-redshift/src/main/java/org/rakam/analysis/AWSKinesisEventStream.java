package org.rakam.analysis;

import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:38.
 */
public class AWSKinesisEventStream implements EventStream {
    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        return null;
    }
}
