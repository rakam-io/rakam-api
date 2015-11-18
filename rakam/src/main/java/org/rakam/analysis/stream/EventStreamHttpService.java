package org.rakam.analysis.stream;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.StreamResponseAdapter;
import org.rakam.config.ForHttpServer;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

@Path("/stream")
@Api(value = "/stream", tags = "event-stream")
public class EventStreamHttpService extends HttpService {
    private final EventStream stream;
    private final SqlParser sqlParser;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public EventStreamHttpService(EventStream stream) {
        this.stream = stream;
        this.sqlParser = new SqlParser();
    }

    @GET
    @ApiOperation(value = "Subscribe Event Stream",
            consumes = "text/event-stream",
            produces = "text/event-stream",
            notes = "Subscribes the event stream periodically to the client.")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/subscribe")
    @IgnoreApi
    public void subscribe(RakamHttpRequest request) {
        if (!Objects.equals(request.headers().get(HttpHeaders.Names.ACCEPT), "text/event-stream")) {
            request.response("the response should accept text/event-stream", HttpResponseStatus.NOT_ACCEPTABLE).end();
            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", HttpResponseStatus.BAD_REQUEST))).end();
            return;
        }

        StreamQuery query;
        try {
            query = JsonHelper.readSafe(data.get(0), StreamQuery.class);
        } catch (IOException e) {
            response.send("result", encode(errorMessage("json couldn't parsed", HttpResponseStatus.BAD_REQUEST))).end();
            return;
        }
        List<CollectionStreamQuery> collect;
        try {
            collect = query.collections.stream().map(collection -> {
                Expression expression = null;
                try {
                    expression = collection.filter == null ? null : sqlParser.createExpression(collection.filter);
                } catch (ParsingException e) {
                    ObjectNode obj = errorMessage(format("Couldn't parse %s: %s", collection.filter, e.getErrorMessage()), HttpResponseStatus.BAD_REQUEST);
                    request.response(encode(obj)).end();
                    throw e;
                }
                return new CollectionStreamQuery(collection.name, expression);
            }).collect(Collectors.toList());
        } catch (ParsingException e) {
            return;
        }

        EventStream.EventStreamer subscribe = stream.subscribe(query.project, collect, query.columns,
                new StreamResponseAdapter(response));

        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(response.isClosed()) {
                    subscribe.shutdown();
                }else {
                    subscribe.sync();
                    eventLoopGroup.schedule(this, 3, TimeUnit.SECONDS);
                }
            }
        }, 3, TimeUnit.SECONDS);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public static class StreamQuery {
        public final String project;
        public final List<StreamQueryRequest> collections;
        public final List<String> columns;

        @JsonCreator
        public StreamQuery(@JsonProperty("project") String project,
                           @JsonProperty("collections") List<StreamQueryRequest> collections,
                           @JsonProperty("columns") List<String> columns) {
            this.project = project;
            this.collections = collections;
            this.columns = columns;
        }
    }

    public static class StreamQueryRequest {
        public final String name;
        public final String filter;

        @JsonCreator
        public StreamQueryRequest(@JsonProperty("name") String name,
                                  @JsonProperty("filter") String filter) {
            this.name = name;
            this.filter = filter;
        }
    }
}
