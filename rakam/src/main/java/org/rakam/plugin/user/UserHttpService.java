package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.inject.Inject;
import org.rakam.plugin.AbstractUserService;
import org.rakam.plugin.AbstractUserService.CollectionEvent;
import org.rakam.plugin.Column;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.plugin.UserStorage;
import org.rakam.plugin.UserStorage.Sorting;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.jsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
@Path("/user")
@Api(value = "/user", description = "User", tags = "user")
public class UserHttpService extends HttpService {
    private final UserPluginConfig config;
    private final SqlParser sqlParser;
    private final AbstractUserService service;

    @Inject
    public UserHttpService(UserPluginConfig config, AbstractUserService service) {
        this.service = service;
        this.config = config;
        this.sqlParser = new SqlParser();
    }

    @JsonRequest
    @ApiOperation(value = "Create new user")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/create")
    public String create(@ApiParam(name = "project", required = true) String project,
                         @ApiParam(name = "properties", required = true) Map<String, Object> properties) {
        try {
            service.create(project, properties);
        } catch (Exception e) {
            return encode(jsonObject()
                    .put("success", false)
                    .put("error", e.getMessage()));
        }

        return encode(jsonObject().put("success", true));
    }

    @JsonRequest
    @ApiOperation(value = "Get user storage metadata")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/metadata")
    public MetadataResponse metadata(@ApiParam(name = "project", required = true) String project) {
        return new MetadataResponse(config.getIdentifierColumn(), service.getMetadata(project));
    }

    public static class MetadataResponse {
        public final List<Column> columns;
        public final String identifierColumn;

        public MetadataResponse(String identifierColumn, List<Column> columns) {
            this.identifierColumn = identifierColumn;
            this.columns = columns;
        }
    }

    /**
     * curl 'http://localhost:9999/user/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "offset": 100, "filter": "age > 30"}'
     */
    @JsonRequest
    @ApiOperation(value = "Search users")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/search")
    public CompletableFuture<QueryResult> search(@ApiParam(name = "project", required = true) String project,
                                                 @ApiParam(name = "filter", required = false) String filter,
                                                 @ApiParam(name = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                                                 @ApiParam(name = "sorting", required = false) Sorting sorting,
                                                 @ApiParam(name = "offset", required = false) int offset,
                                                 @ApiParam(name = "limit", required = false) int limit) {
        Expression expression;
        if (filter != null) {
            try {
                synchronized (sqlParser) {
                    expression = sqlParser.createExpression(filter);
                }
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", filter), 400);
            }
        } else {
            expression = null;
        }

        return service.filter(project, expression, event_filter, sorting, limit, offset);
    }

    /**
     * curl 'http://localhost:9999/user/get_events' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "user": 100}'
     */
    @POST
    @JsonRequest
    @ApiOperation(value = "Get events of the user")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/get_events")
    public CompletableFuture<List<CollectionEvent>> getEvents(@ApiParam(name = "project", required = true) String project,
                                                                                  @ApiParam(name = "user", required = true) String user,
                                                                                  @ApiParam(name = "limit", required = false) Integer limit,
                                                                                  @ApiParam(name = "offset", required = false) Long offset) {
        return service.getEvents(project, user, limit == null ? 15 : limit, offset == null ? 0 : offset);
    }

    /**
     * curl 'http://localhost:9999/user/get' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "user": 100}'
     */
    @JsonRequest
    @ApiOperation(value = "Get user")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/get")
    public CompletableFuture<org.rakam.plugin.user.User> getUser(@ApiParam(name = "project", required = true) String project,
                                                                 @ApiParam(name = "user", required = true) String user) {
        return service.getUser(project, user);
    }

    @JsonRequest
    @ApiOperation(value = "Set user property")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/set_property")
    public JsonResponse setUserProperty(@ApiParam(name = "project", required = true) String project,
                                        @ApiParam(name = "user", required = true) Object user,
                                        @ApiParam(name = "property", required = true) String property,
                                        @ApiParam(name = "value", required = true) Object value) {
        service.setUserProperty(project, user, property, value);
        return JsonResponse.success();
    }
}
