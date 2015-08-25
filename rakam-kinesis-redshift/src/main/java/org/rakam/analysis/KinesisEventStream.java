package org.rakam.analysis;

import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 18:55.
 */
public class KinesisEventStream implements EventStream {
    public KinesisEventStream() {
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {

        return null;
    }
}
