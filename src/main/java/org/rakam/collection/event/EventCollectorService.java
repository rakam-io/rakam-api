package org.rakam.collection.event;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.rakam.model.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.server.RouteMatcher;
import org.rakam.server.http.HttpService;
import org.rakam.util.JsonHelper;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 21:48.
 */
public class EventCollectorService implements HttpService {
    public final ExecutorService executor = new ThreadPoolExecutor(35, 50, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());
    private final Producer kafkaProducer;
    private final Set<EventProcessor> processors;
    private final Set<EventMapper> mappers;

    @Inject
    public EventCollectorService(Producer producer, Set<EventMapper> eventMappers, Set<EventProcessor> eventProcessors) {
        this.mappers = eventMappers;
        this.processors = eventProcessors;
        this.kafkaProducer = producer;
    }

    @Override
    public String getEndPoint() {
        return "/event";
    }

    @Override
    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {
        routeMatcher.add("/collect", HttpMethod.POST, request ->
                request.bodyHandler(buff -> CompletableFuture.supplyAsync(() -> {
                    Event event;
                    try {
                        event = JsonHelper.read(buff, Event.class);
                    } catch (IOException e) {
                        request.response("0").end();
                        return false;
                    }
                    kafkaProducer.send(new KeyedMessage("event", event));
                    return true;
                }, executor)
            .thenAccept(result -> request.response(result ? "1" : "0").end())));
    }
}
