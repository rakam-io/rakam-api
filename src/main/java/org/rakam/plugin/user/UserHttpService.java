package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.plugin.user.storage.Column;
import org.rakam.plugin.user.storage.UserStorage;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.server.http.HttpServer.returnError;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
@Path("/user")
public class UserHttpService implements HttpService {
    private final UserStorage storage;
    private final UserPluginConfig config;
    private final EventSchemaMetastore metastore;

    @Inject
    public UserHttpService(UserPluginConfig config, UserStorage storage, EventSchemaMetastore metastore) {
        this.storage = storage;
        this.config = config;
        this.metastore = metastore;
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

    @JsonRequest
    @Path("/metadata")
    public Object metadata(JsonNode query) {
        JsonNode project = query.get("project");
        if(project == null) {
            return errorMessage("project parameter is required", 400);
        }
        return new JsonObject() {
            public final List<Column> columns = storage.getMetadata(project.asText());
            public final String identifierColumn = config.getIdentifierColumn();
        };
    }

    @JsonRequest
    @Path("/filter")
    public QueryResult filter(FilterQuery query) {
        QueryResult result = storage.filter(query.project, query.filters, query.limit, query.offset);
        result.setProperty("identifierColumn", config.getIdentifierColumn());
        return result;
    }

    @POST
    @Path("/get")
    public void get(RakamHttpRequest request) {
        request.bodyHandler(str -> {
            GetUserEvents query = JsonHelper.read(str, GetUserEvents.class);
            String sqlQuery = metastore.getSchemas(query.project)
                    .entrySet().stream()
                    .filter(c -> c.getValue().stream().anyMatch(f -> f.getName().equals("user")))
                    .map(f -> format("select * from %s", f.getKey()))
                    .collect(Collectors.joining(" union "));
            request.response(format("select user from (%s) order by time limit 100", sqlQuery)).end();
        });
    }

    public static class CreateUserQuery {
        public final String project;
        public final Map<String, Object> properties;

        public CreateUserQuery(String project, Map<String, Object> properties) {
            this.project = project;
            this.properties = properties;
        }
    }

    public static class GetUserEvents {
        public final String project;
        public final Object user;

        @JsonCreator
        public GetUserEvents(@JsonProperty("project") String project,
                             @JsonProperty("user") Object user) {
            this.user = user;
            this.project = project;
        }
    }

    public static class FilterQuery {
        public final String project;
        public final List<FilterCriteria> filters;
        public final int offset;
        public final int limit;

        @JsonCreator
        public FilterQuery(@JsonProperty("project") String project,
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
