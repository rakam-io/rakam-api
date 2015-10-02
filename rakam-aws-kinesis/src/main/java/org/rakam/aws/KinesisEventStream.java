package org.rakam.aws;

import org.rakam.kume.Member;
import org.rakam.kume.ServiceContext;
import org.rakam.kume.service.Service;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class KinesisEventStream extends Service implements EventStream {

    private final ServiceContext<KinesisEventStream> ctx;

    public KinesisEventStream(ServiceContext<KinesisEventStream> ctx) {
        this.ctx = ctx;
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        String randomId = UUID.randomUUID().toString();
        StreamQuery ticket = new StreamQuery(randomId, null, project, collections);

        ctx.sendAllMembers(ticket);

        return new EventStreamer() {
            @Override
            public synchronized void sync() {
                try {
                    Map<Member, CompletableFuture<Object>> map = ctx.askAllMembers(new FetchQuery(randomId));
                    String json = map.values().stream()
                            .map(e -> e.join())
                            .filter(e -> e != null)
                            .flatMap(o -> ((List<String>) o).stream())
                            .collect(Collectors.joining(","));
                    response.send("data", "["+json+"]");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void shutdown() {
                ctx.askAllMembers(randomId);
            }
        };
    }

    @Override
    public void onClose() {

    }
}
