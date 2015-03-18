package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.plugin.user.storage.Column;
import org.rakam.plugin.user.storage.UserStorage;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.server.http.ForHttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.server.http.HttpServer.returnError;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
@Path("/user")
public class UserHttpService implements HttpService {
    private final UserStorage storage;
    private final UserPluginConfig config;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public UserHttpService(UserPluginConfig config, UserStorage storage) {
        this.storage = storage;
        this.config = config;
    }

    public boolean handle(ObjectNode json) {
        String project = json.get("project").asText();
        String actorId = json.get("id").asText();

        if(project==null || actorId==null) {
            return false;
        }

        JsonNode properties = json.get("properties");
        if(properties!=null) {
        }

        return true;
    }

    @POST
    @Path("/create")
    public void collect(RakamHttpRequest request) {
        request.bodyHandler(jsonStr -> {
            CreateUserQuery data;
            try {
                data = JsonHelper.readSafe(jsonStr, CreateUserQuery.class);
            } catch (IOException e) {
                returnError(request, "invalid request", 400);
                return;
            }
            try {
                storage.create(data.project, data.properties);
            } catch (Exception e) {
                request.response("0").end();
                return;
            }

            request.response("1").end();
        });
    }

    @GET
    @JsonRequest
    @Path("/metadata")
    public JsonObject metadata(String project) {
        return new JsonObject() {
            public final List<Column> columns = storage.getMetadata(project);
            public final String identifierColumn = config.getIdentifierColumn();
        };
    }

    @GET
    @Path("/filter")
    public void filter(RakamHttpRequest request) {
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

        Query query;
        try {
            query = JsonHelper.readSafe(data.get(0), Query.class);
        } catch (IOException e) {
            response.send("result", encode(errorMessage("invalid json: "+e.getMessage(), 400))).end();
            return;
        }

        QueryResult queryExecution = storage.filter(query.project, query.filters, query.limit, query.offset);

        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(queryExecution.isFinished()) {
                    QueryResult result = queryExecution.getResult().join();
                    if(result.isFailed()) {
                        response.send("fail", result.getError()).end();
                    }else {
                        result.setProperty("identifierColumn", config.getIdentifierColumn());
                        response.send("result", result).end();
                    }
                }else {
                    response.send("stats", encode(queryExecution.currentStats()));
                    eventLoopGroup.schedule(this, 50, TimeUnit.MILLISECONDS);
                }
            }
        }, 50, TimeUnit.MILLISECONDS);
    }



    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    private String checkAndGetField(ObjectNode json, String fieldName, RakamHttpRequest.StreamResponse response) {
        JsonNode field = json.get(fieldName);
        if (field == null || !field.isTextual()) {
            response.send("result", encode(errorMessage(fieldName+" parameter is required", 400))).end();
            return null;
        }
        return field.textValue();
    }

    public static class CreateUserQuery {
        public final String project;
        public final Map<String, Object> properties;

        public CreateUserQuery(String project, Map<String, Object> properties) {
            this.project = project;
            this.properties = properties;
        }
    }

    public static class Query {
        public final String project;
        public final List<FilterCriteria> filters;
        public final int offset;
        public final int limit;

        @JsonCreator
        public Query(@JsonProperty("project") String project,
                     @JsonProperty("filters") List<FilterCriteria> filters,
                     @JsonProperty("offset") int offset,
                     @JsonProperty("limit") int limit) {
            this.project = project;
            this.filters = filters;
            this.offset = offset;
            this.limit = limit == 0 ? 25 : limit;
            checkArgument(limit <= 1000, "limit must be lower than 1000");
        }
    }

    public interface JsonObject {

    }
}
