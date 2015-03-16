package org.rakam.plugin.user;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.util.JsonHelper;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
@Path("/user")
public class ActorCollectorService implements HttpService {
    private final UserStorage storage;

    @Inject
    public ActorCollectorService(UserStorage storage) {
        this.storage = storage;
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
    @Path("/collect")
    public void collect(RakamHttpRequest request) {
        final ObjectNode json = JsonHelper.generate(request.params());
        request.response(handle(json) ? "1" : "0");
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

        ObjectNode json;
        try {
            json = JsonHelper.readSafe(data.get(0));
        } catch (IOException e) {
            response.send("result", encode(errorMessage("json couldn't parsed", 400))).end();
            return;
        }

        JsonNode queryNode = json.get("query");
        if (queryNode == null && !queryNode.isArray()) {
            response.send("result", encode(errorMessage("query parameter is required", 400))).end();
            return;
        }

        for (JsonNode condition : queryNode) {
            condition.get("operator");
            condition.get("field");
            condition.get("value");
        }

        String columns = checkAndGetField(json, "columns", response);
        String project = checkAndGetField(json, "project", response);
    }

    private String checkAndGetField(ObjectNode json, String fieldName, RakamHttpRequest.StreamResponse response) {
        JsonNode field = json.get(fieldName);
        if (field == null || !field.isTextual()) {
            response.send("result", encode(errorMessage(fieldName+" parameter is required", 400))).end();
            return null;
        }
        return field.textValue();
    }
}
