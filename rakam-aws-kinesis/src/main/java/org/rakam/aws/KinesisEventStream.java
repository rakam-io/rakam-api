package org.rakam.aws;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 18:55.
 */
public class KinesisEventStream implements EventStream {
    private final HazelcastInstance client;

    public KinesisEventStream() {
        client = HazelcastClient.newHazelcastClient();
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        SqlParser sqlParser = new SqlParser();
        Expression expression = sqlParser.createExpression("ali = true");
        return null;
    }
}
