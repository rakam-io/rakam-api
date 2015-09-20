package org.rakam.aws;

import com.google.inject.Inject;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.kume.Cluster;
import org.rakam.kume.ServiceContext;
import org.rakam.kume.service.Service;
import org.rakam.kume.transport.OperationContext;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/15 18:55.
 */
public class KinesisEventStream implements EventStream {
    private final EventStreamService rpcService;
    private final Metastore metastore;

    @Inject
    public KinesisEventStream(Cluster cluster, Metastore metastore) {
        this.metastore = metastore;
        rpcService = cluster.getService("eventListener");
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        return rpcService.publish(response, project, collections);
    }

    public static class EventStreamService extends Service {
        private final Map<String, Queue<String>> connectionMap;
        private final ServiceContext bus;

        public EventStreamService(ServiceContext bus, Metastore metastore) {
            this.bus = bus;
            connectionMap = new ConcurrentHashMap<>();
        }

        @Override
        public void handle(OperationContext ctx, Object object) {
            if (!(object instanceof List)) {
                return;
            }
            List<Map.Entry<String, String>> data = (List<Map.Entry<String, String>>) object;
            data.forEach(entry -> Optional.ofNullable(connectionMap.get(entry.getKey()))
                    .ifPresent(map -> map.add(entry.getValue())));

            List<String> deletedTickets = data.stream()
                    .filter(connectionMap::containsKey)
                    .map(d -> d.getKey())
                    .collect(Collectors.toList());
            ctx.reply(deletedTickets);
        }

        public EventStreamer publish(StreamResponse response, String project, List<CollectionStreamQuery> collections) {
            if(!bus.getCluster().getLocalMember().isClient()) {
                throw new UnsupportedOperationException();
            }

            String randomId = UUID.randomUUID().toString();
            InetSocketAddress address = bus.getCluster().getLocalMember().getAddress();
            StreamQuery ticket = new StreamQuery(randomId, address.getHostName()+":"+address.getPort(), project, collections);

            ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
            connectionMap.put(ticket.id, queue);
            bus.sendAllMembers(ticket);

            return new EventStreamer() {
                @Override
                public synchronized void sync() {
                    if (queue.isEmpty()) {
                        response.send("data", "[]");
                        return;
                    }

                    String batch = queue.poll();
                    while(batch != null) {
                        response.send("data", batch);
                        batch = queue.poll();
                    }
                }

                @Override
                public void shutdown() {
                    connectionMap.remove(ticket.id);
                }
            };
        }

        @Override
        public void onClose() {

        }
    }
}
