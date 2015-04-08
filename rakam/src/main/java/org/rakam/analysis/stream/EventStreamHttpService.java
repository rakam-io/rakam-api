package org.rakam.analysis.stream;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.StreamResponseAdapter;
import org.rakam.config.ForHttpServer;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.util.JsonHelper;

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

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:49.
 */
@Path("/stream")
public class EventStreamHttpService extends HttpService {
    private final EventStream stream;
    private final SqlParser sqlParser;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public EventStreamHttpService(EventStream stream) {
        this.stream = stream;
        this.sqlParser = new SqlParser();
    }

    /**
     * @api {get} /stream/subscribe Subscribe Event Stream
     * @apiVersion 0.1.0
     * @apiName SubscribeStream
     * @apiGroup stream
     * @apiDescription Subscribes the event stream periodically to the client.
     *
     * @apiError Project does not exist.
     * @apiError User does not exist.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {"messages": [{"id": 1, "content": "Hello there!", "seen": false}]}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String[]} [columns]    The columns that will be fetched to the user. All columns will be returned if this parameter is not specified.
     * @apiParam {Object[]} collections    The query that specifies collections that will be fetched
     * @apiParam {String} collections.collection    Collection identifier name
     * @apiParam {String} [collections.filter]    The SQL predicate expression that will filter for the events of the collection
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "collections": [{collection: "pageView", filter: "url LIKE 'http://rakam.io/docs%'"}]}
     *
     * @apiSuccess (200) {Object[]} result  List of events. The fields are dynamic based on the collection schema.
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/stream/subsribe/get?data={"project": "projectId", "collections": [{collection: "pageView", filter: "url LIKE 'http://rakam.io/docs%'"}]}' -H 'Content-Type: text/event-stream;charset=UTF-8'
     */
    @GET
    @Path("/subscribe")
    public void subscribe(RakamHttpRequest request) {
        if (!Objects.equals(request.headers().get(HttpHeaders.Names.ACCEPT), "text/event-stream")) {
            request.response("the response should accept text/event-stream", HttpResponseStatus.NOT_ACCEPTABLE).end();
            return;
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
        List<CollectionStreamQuery> collect;
        try {
            collect = query.collections.stream().map(collection -> {
                Expression expression = null;
                try {
                    expression = collection.filter == null ? null : sqlParser.createExpression(collection.filter);
                } catch (ParsingException e) {
                    ObjectNode obj = errorMessage(format("Couldn't parse %s: %s", collection.filter, e.getErrorMessage()), 400);
                    request.response(encode(obj)).end();
                    throw e;
                }
                return new CollectionStreamQuery(collection.name, expression);
            }).collect(Collectors.toList());
        } catch (ParsingException e) {
            return;
        }

        EventStream.EventStreamer subscribe = stream.subscribe(query.project, collect, query.columns, new StreamResponseAdapter(response));

        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(response.isClosed()) {
                    subscribe.shutdown();
                }else {
                    subscribe.sync();
                    eventLoopGroup.schedule(this, 3, TimeUnit.SECONDS);
                }
                subscribe.shutdown();
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
