package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.plugin.Column;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.plugin.UserStorage;
import org.rakam.plugin.UserStorage.Sorting;
import org.rakam.report.PrestoConfig;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.jsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
@Path("/user")
public class UserHttpService extends HttpService {
    private final UserStorage storage;
    private final UserPluginConfig config;
    private final EventSchemaMetastore metastore;
    private final SqlParser sqlParser;
    private final QueryExecutor executor;
    private final PrestoConfig prestoConfig;

    @Inject
    public UserHttpService(UserPluginConfig config, UserStorage storage, QueryExecutor executor, PrestoConfig prestoConfig, EventSchemaMetastore metastore) {
        this.storage = storage;
        this.config = config;
        this.metastore = metastore;
        this.executor = executor;
        this.prestoConfig = prestoConfig;
        this.sqlParser = new SqlParser();
    }

    public boolean handle(ObjectNode json) {
        String project = json.get("project").asText();
        String actorId = json.get("id").asText();

        if (project == null || actorId == null) {
            return false;
        }

        JsonNode properties = json.get("properties");
        if (properties != null) {
        }

        return true;
    }

    /**
     * @api {post} /user/create Create new user
     * @apiVersion 0.1.0
     * @apiName CreateUser
     * @apiGroup user
     * @apiDescription Creates user to specified user database implementation
     * @apiError StorageError User database returned an error.
     * @apiError Project does not exist.
     * @apiErrorExample {json} Error-Response:
     * HTTP/1.1 500 Internal Server Error
     * {"success": false, "message": "Error Message"}
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"success": true}
     * @apiParam {String} project   Project tracker code that the user belongs.
     * @apiParam {Object} properties    The properties of the user.
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "properties": { "name": "John", "age": 40} }'
     */
    @POST
    @Path("/create")
    public void create(RakamHttpRequest request) {
        request.bodyHandler(jsonStr -> {
            CreateUserQuery data;
            try {
                data = JsonHelper.readSafe(jsonStr, CreateUserQuery.class);
            } catch (IOException e) {
                request.response(encode(errorMessage("Invalid Request: " + e.getMessage(), 400))).end();
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
     * @apiError StorageError User database returned an error.
     * @apiError Project does not exist.
     * @apiErrorExample {json} Error-Response:
     * HTTP/1.1 500 Internal Server Error
     * {"success": false, "message": "Error Message"}
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"identifierColumn": "id", "columns": [{"name": "name", "type": "STRING", "unique": true}]}
     * @apiParam {String} project   Project tracker code
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @Path("/metadata")
    public Object metadata(JsonNode query) {
        JsonNode project = query.get("project");
        if (project == null) {
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
     * @apiErrorExample {json} Error-Response:
     * HTTP/1.1 400 Bad Request
     * {"success": false, "message": "Error Message"}
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {}
     * @apiParam {String} project   Project tracker code
     * @apiParam {Number} offset   Offset results
     * @apiParam {Number} limit   Limit results
     * @apiParam {String} [filter]  SQL Predicate that will be applied to user data
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "offset": 100, "filter": "age > 30"}'
     */
    @JsonRequest
    @Path("/search")
    public QueryResult search(UserQuery query) {
        Expression expression;
        if (query.filter != null) {
            try {
                expression = sqlParser.createExpression(query.filter);
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", query.filter), 400);
            }
        } else {
            expression = null;
        }
        QueryResult result = storage.filter(query.project, expression, query.sorting, query.limit, query.offset);
        return result;
    }

    /**
     * @api {post} /user/get_events Get events of the user
     * @apiVersion 0.1.0
     * @apiName GetEventsOfUser
     * @apiGroup user
     * @apiDescription Returns events of the user.
     * @apiError User does not exist.
     * @apiErrorExample {json} Error-Response:
     * HTTP/1.1 400 Bad Request
     * {"success": false, "message": "User does not exist."}
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {}
     * @apiParam {String} project   Project tracker code
     * @apiParam {Number} user   User Id
     * @apiParam {Number} limit   Limit results
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/user/get_events' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "user": 100}'
     */
    @POST
    @JsonRequest
    @Path("/get_events")
    public CompletableFuture<String> getEvents(UserSpecificRequest query) {
        String sqlQuery = metastore.getSchemas(query.project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("user")))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("time")))
                .map(entry ->
                        format("select '{\"_collection\": \"%s\",'||'", entry.getKey()) + entry.getValue().stream()
                                .filter(field -> !field.getName().equals("user"))
                                .map(field -> {
                                    switch (field.getType()) {
                                        case LONG:
                                        case DOUBLE:
                                        case BOOLEAN:
                                            return format("\"%1$s\": '||COALESCE(cast(%1$s as varchar), 'null')||'", field.getName());
                                        default:
                                            return format("\"%1$s\": \"'||COALESCE(replace(try_cast(%1$s as varchar), '\n', '\\n'), 'null')||'\"", field.getName());
                                    }

                                })
                                .collect(Collectors.joining(", ")) +
                                format(" }' as json, time from %s where user = %s",
                                        prestoConfig.getColdStorageConnector() + "." + query.project + "." + entry.getKey(),
                                        query.user))
                .collect(Collectors.joining(" union all "));


        return executor.executeQuery("", format("select json from (%s) order by time desc limit %d", sqlQuery, 10)).getResult()
                .thenApply(result ->
                        "[" + result.getResult().stream()
                                .map(s -> (String) s.get(0)).collect(Collectors.joining(",")) + "]");
    }

    /**
     * @api {post} /user/get Get user
     * @apiVersion 0.1.0
     * @apiName GetUser
     * @apiGroup user
     * @apiDescription Returns user that has the specified id.
     * @apiError User does not exist.
     * @apiErrorExample {json} Error-Response:
     * HTTP/1.1 400 Bad Request
     * {"success": false, "message": "User does not exist."}
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {}
     * @apiParam {String} project   Project tracker code
     * @apiParam {Number} user   User Id
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/user/get' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 100}'
     */
    @JsonRequest
    @Path("/get")
    public org.rakam.plugin.user.User getUser(UserSpecificRequest userRequest) {
        return storage.getUser(userRequest.project, userRequest.user);
    }


    /**
     * @api {post} /user/set_property Set user property
     * @apiVersion 0.1.0
     * @apiName SetUserProperty
     * @apiGroup user
     * @apiDescription Sets or updates (if it exists) a user property
     * @apiError User does not exist.
     * @apiErrorExample {json} Error-Response:
     * HTTP/1.1 400 Bad Request
     * {"success": false, "message": "User does not exist."}
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {}
     * @apiParam {String} project   Project tracker code
     * @apiParam {Number} user   User Id
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/user/get' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 100}'
     */
    @JsonRequest
    @Path("/set_property")
    public JsonResponse setUserProperty(SetUserProperty userRequest) {
        storage.setUserProperty(userRequest.project, userRequest.user, userRequest.property, userRequest.value);
        return new JsonResponse() {
            public final boolean success = true;
        };
    }

    public static class CreateUserQuery {
        public final String project;
        public final Map<String, Object> properties;

        public CreateUserQuery(String project, Map<String, Object> properties) {
            this.project = project;
            this.properties = properties;
        }
    }

    public static class SetUserProperty {
        public final String project;
        public final Object user;
        public final String property;
        public final Object value;

        @JsonCreator
        public SetUserProperty(@JsonProperty("project") String project,
                               @JsonProperty("user") Object user,
                               @JsonProperty("property") String property,
                               @JsonProperty("value") Object value) {
            this.project = project;
            this.user = user;
            this.property = property;
            this.value = value;
        }
    }

    public static class UserSpecificRequest {
        public final String project;
        public final Object user;

        @JsonCreator
        public UserSpecificRequest(@JsonProperty("project") String project,
                                   @JsonProperty("user") Object user) {
            this.user = checkNotNull(user, "user is required");
            this.project = checkNotNull(project, "project is required");
        }
    }

    public static class UserQuery {
        public final String project;
        public final String filter;
        public final Sorting sorting;
        public final int offset;
        public final int limit;

        @JsonCreator
        public UserQuery(@JsonProperty("project") String project,
                         @JsonProperty("filter") String filter,
                         @JsonProperty("sorting") Sorting sorting,
                         @JsonProperty("offset") int offset,
                         @JsonProperty("limit") int limit) {
            this.project = checkNotNull(project, "project is required");
            this.filter = filter;
            this.sorting = sorting;
            this.offset = offset;
            this.limit = limit == 0 ? 25 : limit;
            checkArgument(limit <= 1000, "limit must be lower than 1000");
        }
    }

}
