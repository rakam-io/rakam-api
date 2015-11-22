package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.AbstractUserService;
import org.rakam.plugin.AbstractUserService.CollectionEvent;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.plugin.UserPropertyMapper;
import org.rakam.plugin.UserStorage;
import org.rakam.plugin.UserStorage.Sorting;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.server.http.HttpServer.returnError;

@Path("/user")
@Api(value = "/user", description = "User", tags = "user")
public class UserHttpService extends HttpService {
    final static Logger LOGGER = Logger.get(UserHttpService.class);
    private final byte[] OK_MESSAGE = "1".getBytes(Charset.forName("UTF-8"));

    private final UserPluginConfig config;
    private final SqlParser sqlParser;
    private final AbstractUserService service;
    private final Set<UserPropertyMapper> mappers;

    @Inject
    public UserHttpService(UserPluginConfig config, Set<UserPropertyMapper> mappers, AbstractUserService service) {
        this.service = service;
        this.config = config;
        this.sqlParser = new SqlParser();
        this.mappers = mappers;
    }

    @JsonRequest
    @ApiOperation(value = "Create new user")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/create")
    public String create(@ParamBody User user) {
        try {
            return service.create(user.project, user.properties);
        } catch (Exception e) {
            throw new RakamException(e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create new user")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/batch/create")
    public List<String> batchCreate(@ApiParam(name = "project") String project, @ApiParam(name = "users") List<User> users) {
        try {
            return service.batchCreate(project, users);
        } catch (Exception e) {
            throw new RakamException(e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get user storage metadata")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/metadata")
    public MetadataResponse getMetadata(@ApiParam(name = "project", required = true) String project) {
        return new MetadataResponse(config.getIdentifierColumn(), service.getMetadata(project));
    }

    public static class MetadataResponse {
        public final List<SchemaField> columns;
        public final String identifierColumn;

        public MetadataResponse(String identifierColumn, List<SchemaField> columns) {
            this.identifierColumn = identifierColumn;
            this.columns = columns;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Search users")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/search")
    public CompletableFuture<QueryResult> searchUsers(@ApiParam(name = "project") String project,
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
                throw new RakamException(format("filter expression '%s' couldn't parsed", filter),
                        HttpResponseStatus.BAD_REQUEST);
            }
        } else {
            expression = null;
        }

        return service.filter(project, expression, event_filter, sorting, limit, offset);
    }

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

    public static class SetUserProperties {
        public final String project;
        public final String user;
        public final Map<String, Object> properties;

        public SetUserProperties(@ApiParam(name = "project") String project,
                                 @ApiParam(name = "user") String user,
                                 @ApiParam(name = "properties") Map<String, Object> properties) {
            this.project = project;
            this.user = user;
            this.properties = properties;
        }
    }

    @ApiOperation(value = "Set user properties", request = SetUserProperties.class, response = Integer.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/set_property")
    @POST
    public void setUserProperties(RakamHttpRequest request) {
        request.bodyHandler(s -> {
            SetUserProperties req;
            try {
                req = JsonHelper.readSafe(s, SetUserProperties.class);
            } catch (IOException e) {
                returnError(request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            }

            if(!mapProperties(req, request)) {
                return;
            }

            service.setUserProperties(req.project, req.user, req.properties);
            request.response(OK_MESSAGE).end();
        });
    }

    private boolean mapProperties(SetUserProperties req, RakamHttpRequest request) {
        InetAddress socketAddress = ((InetSocketAddress) request.context().channel()
                .remoteAddress()).getAddress();

        for (UserPropertyMapper mapper : mappers) {
            try {
                mapper.map(req.project, req.properties, request.headers(), socketAddress);
            } catch (Exception e) {
                LOGGER.error(e);
                request.response("0", BAD_REQUEST).end();
                return false;
            }
        }

        return true;
    }

    @JsonRequest
    @ApiOperation(value = "Set user properties once", request = SetUserProperties.class, response = Integer.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/set_once")
    public void setUserPropertiesOnce(RakamHttpRequest request) {
        request.bodyHandler(s -> {
            SetUserProperties req;
            try {
                req = JsonHelper.readSafe(s, SetUserProperties.class);
            } catch (IOException e) {
                returnError(request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            }

            if (!mapProperties(req, request)) {
                return;
            }

            // TODO: we may cache these values and reduce the db hit.
            service.setUserPropertiesOnce(req.project, req.user, req.properties);
            request.response(OK_MESSAGE).end();
        });
    }

    @JsonRequest
    @ApiOperation(value = "Set user property")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/increment")
    public JsonResponse incrementUserProperty(@ApiParam(name = "project", required = true) String project,
                                        @ApiParam(name = "user", required = true) String user,
                                        String property, long value) {
        service.incrementProperty(project, user, property, value);
        return JsonResponse.success();
    }
}
