package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.plugin.user.storage.Column;
import org.rakam.plugin.user.storage.UserStorage;
import org.rakam.report.PrestoQueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.json.JsonResponse;

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
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.jsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
@Path("/user")
public class UserHttpService implements HttpService {
    private final UserStorage storage;
    private final UserPluginConfig config;
    private final EventSchemaMetastore metastore;
    private final SqlParser sqlParser;
    private final PrestoQueryExecutor executor;

    @Inject
    public UserHttpService(UserPluginConfig config, UserStorage storage, PrestoQueryExecutor executor, EventSchemaMetastore metastore) {
        this.storage = storage;
        this.config = config;
        this.metastore = metastore;
        this.executor = executor;
        this.sqlParser = new SqlParser();
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

    /**
     * @api {post} /user/create Create new user
     * @apiVersion 0.1.0
     * @apiName CreateUser
     * @apiGroup user
     * @apiDescription Creates user to specified user database implementation
     *
     * @apiError StorageError User database returned an error.
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Error Message"}
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {"success": true}
     *
     * @apiParam {String} project   Project tracker code that the user belongs.
     * @apiParam {Object} properties    The properties of the user.
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "properties": { "name": "John", "age": 40} }'
     */
    @POST
    @Path("/create")
    public void create(RakamHttpRequest request) {
        request.bodyHandler(jsonStr -> {
            CreateUserQuery data;
            try {
                data = JsonHelper.readSafe(jsonStr, CreateUserQuery.class);
            } catch (IOException e) {
                returnError(request, "Invalid Request: "+e.getMessage(), 400);
                return;
            }
            try {
                storage.create(data.project, data.properties);
            } catch (Exception e) {
                String encode = encode(jsonObject()
                        .put("success", false)
                        .put("error", e.getMessage()));
                request.response(encode).end();
                return;
            }

            request.response(encode(jsonObject().put("success", true))).end();
        });
    }

    /**
     * @api {post} /user/metadata Create new user
     * @apiVersion 0.1.0
     * @apiName UserStorageMetadata
     * @apiGroup user
     * @apiDescription Returns user metadata attributes of the specified project tracker code.
     *
     * @apiError StorageError User database returned an error.
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Error Message"}
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {"identifierColumn": "id", "columns": [{"name": "name", "type": "STRING", "unique": true}]}
     *
     * @apiParam {String} project   Project tracker code
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @Path("/metadata")
    public Object metadata(JsonNode query) {
        JsonNode project = query.get("project");
        if(project == null) {
            return errorMessage("project parameter is required", 400);
        }
        return new JsonResponse() {
            public final List<Column> columns = storage.getMetadata(project.asText());
            public final String identifierColumn = config.getIdentifierColumn();
        };
    }

    /**
     * @api {post} /user/search Search users
     * @apiVersion 0.1.0
     * @apiName SearchUser
     * @apiGroup user
     * @apiDescription Returns users that satisfy the specified filter conditions.
     *
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {"success": false, "message": "Error Message"}
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {Number} offset   Offset results
     * @apiParam {Number} limit   Limit results
     * @apiParam {String} [filter]  SQL Predicate that will be applied to user data
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "offset": 100, "filter": "age > 30"}'
     */
    @JsonRequest
    @Path("/search")
    public QueryResult search(FilterQuery query) {
        Expression expression;
        if(query.filter != null) {
            expression = sqlParser.createExpression(query.filter);
        }else {
            expression = null;
        }
        QueryResult result = storage.filter(query.project, expression, query.limit, query.offset);
        return result;
    }

    /**
     * @api {post} /user/get_events Get events of the user
     * @apiVersion 0.1.0
     * @apiName GetEventsOfUser
     * @apiGroup user
     * @apiDescription Returns events of the user.
     *
     * @apiError User does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {"success": false, "message": "User does not exist."}
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {Number} user   User Id
     * @apiParam {Number} limit   Limit results
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "user": 100}'
     */
    @POST
    @Path("/get_events")
    public void get(RakamHttpRequest request) {
        request.bodyHandler(str -> {
            GetUserEvents query = JsonHelper.read(str, GetUserEvents.class);
            String sqlQuery = metastore.getSchemas(query.project)
                    .entrySet().stream()
                    .filter(c -> c.getValue().stream().anyMatch(f -> f.getName().equals("user")))
                    .map(f -> {
                        String collect = "'{'||"+ f.getValue().stream()
                                .map(field -> format("\"%1$s\": try_cast(%1$s as varchar)", field.getName()))
                                .collect(Collectors.joining(", ")) + "|| '}'";
                        return format("select %s as json from %s", collect, f.getKey());
                    })
                    .collect(Collectors.joining(" union "));

            executor.executeQuery(format("select json from (%s) order by time desc limit 100", sqlQuery)).getResult()
                    .thenAccept(result ->
                            request.response("[" +result.getResult().stream()
                                    .map(s -> (String) s.get(0)).collect(Collectors.joining(","))+ "]").end());
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
        public final String filter;
        public final int offset;
        public final int limit;

        @JsonCreator
        public FilterQuery(@JsonProperty("project") String project,
                           @JsonProperty("filter") String filter,
                           @JsonProperty("offset") int offset,
                           @JsonProperty("limit") int limit) {
            this.project = project;
            this.filter = filter;
            this.offset = offset;
            this.limit = limit == 0 ? 25 : limit;
            checkArgument(limit <= 1000, "limit must be lower than 1000");
        }
    }

}
