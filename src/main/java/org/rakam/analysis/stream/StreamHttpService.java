package org.rakam.analysis.stream;

import com.facebook.presto.jdbc.internal.guava.collect.Maps;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.plugin.user.FilterClause;
import org.rakam.server.http.ForHttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.util.JsonHelper;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:49.
 */
@Path("/stream")
public class StreamHttpService implements HttpService {
    private final EventStream stream;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public StreamHttpService(EventStream stream) {
        this.stream = stream;
    }

    @GET
    @Path("/subscribe")
    public void subscribe(RakamHttpRequest request) {
        if (!Objects.equals(request.headers().get(HttpHeaders.Names.ACCEPT), "text/event-stream")) {
//            request.response("the response should accept text/event-stream", HttpResponseStatus.NOT_ACCEPTABLE).end();
//            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse();
        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", 400))).end();
            return;
        }

        StreamQuery query;
        try {
            query = JsonHelper.readSafe(data.get(0), StreamQuery.class);
        } catch (IOException e) {
            response.send("result", encode(errorMessage("json couldn't parsed", 400))).end();
            return;
        }
        Map<String, FilterClause> collect = Maps.newHashMap();
        query.collections.forEach(collection -> collect.put(collection, query.filters.get(collection)));
        Supplier<CompletableFuture<Stream<Map<String, Object>>>> subscribe = stream.subscribe(query.project, collect, query.columns);

        eventLoopGroup.scheduleWithFixedDelay(() -> subscribe.get()
                .thenAccept(result -> {
                    List<Map<String, Object>> collect1 = result.collect(Collectors.toList());
                    response.send("data", collect1);
                }), 0, 3, TimeUnit.SECONDS);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public static class StreamQuery {
        public final String project;
        public final Map<String, FilterClause> filters;
        public final  List<String> columns;
        public final Set<String> collections;

        @JsonCreator
        public StreamQuery(@JsonProperty("project") String project,
                           @JsonProperty("filters") Map<String, FilterClause> filters,
                           @JsonProperty("collections") Set<String> collections,
                           @JsonProperty("columns") List<String> columns) {
            this.project = project;
            this.filters = filters == null ? ImmutableMap.of() : filters;
            this.collections = collections;
            this.columns = columns;
        }
    }
}
