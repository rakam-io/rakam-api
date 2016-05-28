package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.AbstractUserService.CollectionEvent;
import org.rakam.plugin.user.AbstractUserService.PreCalculateQuery;
import org.rakam.plugin.user.UserStorage.Sorting;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.AllowCookie;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Charsets.UTF_8;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.MASTER_KEY;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.server.http.HttpServer.returnError;

@Path("/user")
@Api(value = "/user", nickname = "user", description = "User", tags = "user")
public class UserHttpService extends HttpService {
    private final static Logger LOGGER = Logger.get(UserHttpService.class);
    private final byte[] OK_MESSAGE = "1".getBytes(UTF_8);

    private final UserPluginConfig config;
    private final SqlParser sqlParser;
    private final AbstractUserService service;
    private final Set<UserPropertyMapper> mappers;
    private final Metastore metastore;
    private final QueryHttpService queryService;
    private final ContinuousQueryService continuousQueryService;
    private final ApiKeyService apiKeyService;

    @Inject
    public UserHttpService(UserPluginConfig config,
                           Set<UserPropertyMapper> mappers,
                           Metastore metastore,
                           ApiKeyService apiKeyService,
                           ContinuousQueryService continuousQueryService,
                           AbstractUserService service,
                           QueryHttpService queryService) {
        this.service = service;
        this.config = config;
        this.metastore = metastore;
        this.apiKeyService = apiKeyService;
        this.queryService = queryService;
        this.sqlParser = new SqlParser();
        this.mappers = mappers;
        this.continuousQueryService = continuousQueryService;
    }

    @JsonRequest
    @ApiOperation(value = "Create new user")

    @Path("/create")
    public Object createUser(@BodyParam User user) {
        String project = apiKeyService.getProjectOfApiKey(user.api != null ? user.api.apiKey : null, WRITE_KEY);

        return service.create(project, user.id, user.properties);
    }

    @JsonRequest
    @ApiOperation(value = "Create new users", authorizations = @Authorization(value = "write_key"))

    @Path("/batch/create")
    public List<Object> createUsers(@Named("project") String project, @ApiParam("users") List<User> users) {
        try {
            return service.batchCreate(project, users);
        } catch (Exception e) {
            throw new RakamException(e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
    }

    @GET
    @ApiOperation(value = "Get user storage metadata", authorizations = @Authorization(value = "read_key"))

    @Path("/metadata")
    public MetadataResponse getMetadata(@Named("project") String project) {
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
    @ApiOperation(value = "Search users", authorizations = @Authorization(value = "read_key"))

    @Path("/search")
    public CompletableFuture<QueryResult> searchUsers(@Named("project") String project,
                                                      @ApiParam(value = "columns", required = false) List<String> columns,
                                                      @ApiParam(value = "filter", required = false) String filter,
                                                      @ApiParam(value = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                                                      @ApiParam(value = "sorting", required = false) Sorting sorting,
                                                      @ApiParam(value = "offset", required = false) String offset,
                                                      @ApiParam(value = "limit", required = false) Integer limit) {
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

        limit = limit == null ? 100 : Math.min(5000, limit);

        return service.filter(project, columns, expression, event_filter, sorting, limit, offset);
    }

    @POST
    @JsonRequest
    @ApiOperation(value = "Get events of the user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/get_events")
    public CompletableFuture<List<CollectionEvent>> getEvents(@Named("project") String project,
                                                              @ApiParam("user") String user,
                                                              @ApiParam(value = "limit", required = false) Integer limit,
                                                              @ApiParam(value = "offset", required = false) Instant offset) {
        return service.getEvents(project, user, limit == null ? 15 : limit, offset);
    }

    @POST
    @JsonRequest
    @ApiOperation(value = "Get events of the user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/create_segment")
    public JsonResponse createSegment(@Named("project") String project,
                                      @ApiParam("name") String name,
                                      @ApiParam("table_name") String tableName,
                                      @ApiParam(value = "filter_expression", required = false) String filterExpression,
                                      @ApiParam(value = "event_filters", required = false) List<UserStorage.EventFilter> eventFilters,
                                      @ApiParam("cache_eviction") Duration duration) {
        if (filterExpression == null && (eventFilters == null || eventFilters.isEmpty())) {
            throw new RakamException("At least one predicate is required", BAD_REQUEST);
        }

        Expression expression = null;
        if (filterExpression != null) {
            synchronized (sqlParser) {
                expression = sqlParser.createExpression(filterExpression);
            }
        }

        service.createSegment(project, name, tableName, expression, eventFilters, duration);

        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/get")
    public CompletableFuture<org.rakam.plugin.user.User> getUser(@Named("project") String project,
                                                                 @ApiParam("user") Object user) {
        return service.getUser(project, user);
    }

    @JsonRequest
    @ApiOperation(value = "Merge user with anonymous id", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = { @ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/merge")
    @AllowCookie
    public boolean mergeUser(@Named("project") String project,
                             @ApiParam("user") String user,
                             @ApiParam("api") User.UserContext api,
                             @ApiParam("anonymous_id") String anonymousId,
                             @ApiParam("created_at") Instant createdAt,
                             @ApiParam("merged_at") Instant mergedAt) {
        // TODO: what if a user sends real user ids instead of its previous anonymous id?
        if (!config.getEnableUserMapping()) {
            throw new RakamException("The feature is not supported", HttpResponseStatus.PRECONDITION_FAILED);
        }
        service.merge(project, user, anonymousId, createdAt, mergedAt);
        return true;
    }

    @ApiOperation(value = "Set user properties", request = User.class, response = Integer.class)
    @ApiResponses(value = { @ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/set_properties")
    @POST
    public void setProperties(RakamHttpRequest request) {
        request.bodyHandler(s -> {
            User req;
            try {
                req = JsonHelper.readSafe(s, User.class);
            } catch (IOException e) {
                returnError(request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            }

            String project = apiKeyService.getProjectOfApiKey(req.api.apiKey, WRITE_KEY);

            if (!mapProperties(project, req, request)) {
                return;
            }

            service.setUserProperties(project, req.id, req.properties);
            request.response(OK_MESSAGE).end();
        });
    }

    private boolean mapProperties(String project, User req, RakamHttpRequest request) {
        InetAddress socketAddress = ((InetSocketAddress) request.context().channel()
                .remoteAddress()).getAddress();

        for (UserPropertyMapper mapper : mappers) {
            try {
                mapper.map(project, req.properties, request.headers(), socketAddress);
            } catch (Exception e) {
                LOGGER.error(e);
                request.response("0", BAD_REQUEST).end();
                return false;
            }
        }

        return true;
    }

    @JsonRequest
    @ApiOperation(value = "Set user properties once", request = User.class, response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/set_properties_once")
    public void setPropertiesOnce(RakamHttpRequest request) {
        request.bodyHandler(s -> {
            User req;
            try {
                req = JsonHelper.readSafe(s, User.class);
            } catch (IOException e) {
                returnError(request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            }

            String project = apiKeyService.getProjectOfApiKey(req.api.apiKey, WRITE_KEY);

            if (!mapProperties(project, req, request)) {
                return;
            }

            // TODO: we may cache these values and reduce the db hit.
            service.setUserPropertiesOnce(project, req.id, req.properties);
            request.response(OK_MESSAGE).end();
        });
    }

    @JsonRequest
    @ApiOperation(value = "Set user property", authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/increment_property")
    @AllowCookie
    public JsonResponse incrementProperty(@ApiParam("api") User.UserContext api,
                                          @ApiParam("id") String user,
                                          @ApiParam("property") String property,
                                          @ApiParam("value") double value) {
        String project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
        service.incrementProperty(project, user, property, value);
        return JsonResponse.success();
    }

    @ApiOperation(value = "Create pre-calculate rule",
            authorizations = @Authorization(value = "master_key")
    )
    @Consumes("text/event-stream")
    @IgnoreApi
    @GET
    @Path("/pre_calculate")
    public void precalculateUsers(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, PreCalculateQuery.class,
                service::precalculate, MASTER_KEY, false);
    }

    @JsonRequest
    @ApiOperation(value = "Unset user property")
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/unset_properties")
    @AllowCookie
    public JsonResponse unsetProperty(@ApiParam("api") User.UserContext api,
                                      @ApiParam("id") Object id,
                                      @ApiParam("properties") List<String> properties) {
        String project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
        service.unsetProperties(project, id, properties);
        return JsonResponse.success();
    }
}
