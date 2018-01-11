package org.rakam.analysis.stream;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.ServiceStarter;
import org.rakam.analysis.ApiKeyService;
import org.rakam.http.ForHttpServer;
import org.rakam.plugin.stream.CollectionStreamQuery;
import org.rakam.plugin.stream.EventStream;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCEPT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;
import static java.lang.String.format;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.READ_KEY;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

@Path("/stream")
@Api(value = "/stream", tags = "event-stream")
public class EventStreamHttpService
        extends HttpService {
    private final static Logger LOGGER = Logger.get(ServiceStarter.class);

    private final EventStream stream;
    private final SqlParser sqlParser;
    private final ApiKeyService apiKeyService;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public EventStreamHttpService(EventStream stream, ApiKeyService apiKeyService) {
        this.stream = stream;
        this.apiKeyService = apiKeyService;
        this.sqlParser = new SqlParser();
    }

    @GET
    @ApiOperation(value = "Subscribe Event Stream",
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key"),
            notes = "Subscribes the event stream.")

    @Path("/subscribe")
    @IgnoreApi
    public void subscribe(RakamHttpRequest request) {
        if (!Objects.equals(request.headers().get(ACCEPT), "text/event-stream")) {
            request.response("the response should accept text/event-stream", NOT_ACCEPTABLE).end();
            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("error", encode(errorMessage("data query parameter is required", BAD_REQUEST))).end();
            return;
        }

        StreamQuery query;
        try {
            query = JsonHelper.readSafe(data.get(0), StreamQuery.class);
        } catch (IOException e) {
            response.send("error", encode(errorMessage("JSON couldn't parsed: " + e.getMessage(), BAD_REQUEST)));
            return;
        }
        List<String> api_key = request.params().get("read_key");

        if (api_key == null || api_key.isEmpty()) {
            response.send("error", HttpResponseStatus.FORBIDDEN.reasonPhrase()).end();
            return;
        }

        String project = apiKeyService.getProjectOfApiKey(api_key.get(0), READ_KEY);

        List<CollectionStreamQuery> collect;

        if (query.collections == null) {
            collect = ImmutableList.of(new CollectionStreamQuery(null, null));
        } else {
            try {
                collect = query.collections.stream().map(collection -> {
                    Expression expression = null;
                    try {
                        expression = collection.filter == null ? null : sqlParser.createExpression(collection.filter);
                    } catch (ParsingException e) {
                        request.response(encode(errorMessage(format("Couldn't parse %s: %s",
                                collection.filter, e.getErrorMessage()), BAD_REQUEST))).end();
                        throw e;
                    }
                    return new CollectionStreamQuery(collection.name,
                            expression == null ? null : expression.toString());
                }).collect(Collectors.toList());
            } catch (ParsingException e) {
                return;
            }
        }

        EventStream.EventStreamer subscribe = stream.subscribe(project, collect, query.columns,
                new StreamResponseAdapter(response));

        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if (response.isClosed()) {
                    subscribe.shutdown();
                } else {
                    try {
                        subscribe.sync();
                    } catch (Throwable e) {
                        LOGGER.error(e);
                        subscribe.shutdown();
                    }
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
        public final List<StreamQueryRequest> collections;
        public final List<String> columns;

        @JsonCreator
        public StreamQuery(
                @ApiParam("collections") List<StreamQueryRequest> collections,
                @ApiParam(value = "columns", required = false) List<String> columns) {
            this.collections = collections;
            this.columns = columns;
        }
    }

    public static class StreamQueryRequest {
        public final String filter;
        private final String name;

        @JsonCreator
        public StreamQueryRequest(@ApiParam("name") String name,
                                  @ApiParam(value = "filter", required = false) String filter) {
            this.name = name;
            this.filter = filter;
        }
    }
}
