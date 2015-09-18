package org.rakam.aws;

import com.google.inject.Inject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 18:55.
 */
public class KinesisEventStream implements EventStream {
    private final HazelcastInstance hazelcast;
    private final Map<String, Queue<List<byte[]>>> connectionMap;
    private final ITopic<StreamQuery> topic;

    @Inject
    public KinesisEventStream(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        connectionMap = new ConcurrentHashMap<>();
        topic = hazelcast.getTopic("eventStream");
        hazelcast.getUserContext().put("eventStream", connectionMap);
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        String randomId = UUID.randomUUID().toString();
        String memberId = hazelcast.getCluster().getLocalMember().getUuid();

        ConcurrentLinkedQueue<List<byte[]>> queue = new ConcurrentLinkedQueue<>();
        connectionMap.put(randomId, queue);
        topic.publish(new StreamQuery(randomId, memberId, project, collections));

        return new EventStreamer() {
            @Override
            public void sync() {
                if (!queue.isEmpty()) {
                    System.out.println(1);
                }else {
                    return;
                }
                queue.peek().stream().count();
                connectionMap.get(randomId);
                response.send("data", "");
            }

            @Override
            public void shutdown() {
                connectionMap.remove(randomId);
            }
        };
    }

}
