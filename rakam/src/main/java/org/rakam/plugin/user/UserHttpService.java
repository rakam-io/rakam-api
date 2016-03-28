package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.Subscribe;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ProjectItem;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.user.AbstractUserService.CollectionEvent;
import org.rakam.plugin.user.UserStorage.Sorting;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.AllowCookie;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.inject.Inject;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Charsets.UTF_8;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static java.lang.String.format;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.MASTER_KEY;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.server.http.HttpServer.returnError;

@Path("/user")
@Api(value = "/user", nickname = "user", description = "User", tags = "user")
public class UserHttpService extends HttpService {
    final static Logger LOGGER = Logger.get(UserHttpService.class);
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
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/create")
    @IgnorePermissionCheck
    public String create(@ParamBody User user) {
        if (!apiKeyService.checkPermission(user.project, WRITE_KEY, user.api.writeKey)) {
            throw new RakamException(UNAUTHORIZED);
        }

        try {
            return service.create(user.project, user.id, user.properties);
        } catch (Exception e) {
            throw new RakamException(e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create new user")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/batch/create")
    @IgnorePermissionCheck
    public List<String> batchCreate(@ApiParam(name = "project") String project,
                                    @ApiParam(name = "users") List<User> users) {
        for (User user : users) {
            if (!apiKeyService.checkPermission(user.project, WRITE_KEY, user.api.writeKey)) {
                throw new RakamException(UNAUTHORIZED);
            }
        }

        try {
            return service.batchCreate(project, users);
        } catch (Exception e) {
            throw new RakamException(e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get user storage metadata", authorizations = @Authorization(value = "read_key"))
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
    @ApiOperation(value = "Search users", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/search")
    public CompletableFuture<QueryResult> searchUsers(@ApiParam(name = "project") String project,
                                                      @ApiParam(name = "columns", required = false) List<String> columns,
                                                      @ApiParam(name = "filter", required = false) String filter,
                                                      @ApiParam(name = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                                                      @ApiParam(name = "sorting", required = false) Sorting sorting,
                                                      @ApiParam(name = "offset", required = false) String offset,
                                                      @ApiParam(name = "limit", required = false) Integer limit) {
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
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/get_events")
    public CompletableFuture<List<CollectionEvent>> getEvents(@ApiParam(name = "project", required = true) String project,
                                                              @ApiParam(name = "user", required = true) String user,
                                                              @ApiParam(name = "limit", required = false) Integer limit,
                                                              @ApiParam(name = "offset", required = false) Instant offset) {
        return service.getEvents(project, user, limit == null ? 15 : limit, offset);
    }

    @POST
    @JsonRequest
    @ApiOperation(value = "Get events of the user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/create_segment")
    public JsonResponse createSegment(@ApiParam(name = "project") String project,
                                      @ApiParam(name = "name") String name,
                                      @ApiParam(name = "table_name") String tableName,
                                      @ApiParam(name = "filter_expression", required = false) String filterExpression,
                                      @ApiParam(name = "event_filters", required = false) List<UserStorage.EventFilter> eventFilters,
                                      @ApiParam(name = "cache_eviction") Duration duration) {
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
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/get")
    public CompletableFuture<org.rakam.plugin.user.User> getUser(@ApiParam(name = "project", required = true) String project,
                                                                 @ApiParam(name = "user", required = true) String user) {
        return service.getUser(project, user);
    }

    @JsonRequest
    @ApiOperation(value = "Merge user with anonymous id", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/merge")
    @AllowCookie
    @IgnorePermissionCheck()
    public boolean mergeUser(@ApiParam(name = "project", required = true) String project,
                             @ApiParam(name = "user", required = true) String user,
                             @ApiParam(name = "api") User.UserContext api,
                             @ApiParam(name = "anonymous_id") String anonymousId,
                             @ApiParam(name = "created_at") Instant createdAt,
                             @ApiParam(name = "merged_at") Instant mergedAt) {
        // TODO: what if a user sends real user ids instead of its previous anonymous id?
        if (!apiKeyService.checkPermission(project, WRITE_KEY, api.writeKey)) {
            throw new RakamException(UNAUTHORIZED);
        }
        if (!config.getEnableUserMapping()) {
            throw new RakamException("The feature is not supported", HttpResponseStatus.PRECONDITION_FAILED);
        }
        service.merge(project, user, anonymousId, createdAt, mergedAt);
        return true;
    }

    @ApiOperation(value = "Set user properties", request = SetUserProperties.class, response = Integer.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/set_properties")
    @IgnorePermissionCheck
    @POST
    public void setProperties(RakamHttpRequest request) {
        request.bodyHandler(s -> {
            SetUserProperties req;
            try {
                req = JsonHelper.readSafe(s, SetUserProperties.class);
            } catch (IOException e) {
                returnError(request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            }

            if (!apiKeyService.checkPermission(req.project, WRITE_KEY, req.api.writeKey)) {
                returnError(request, UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
                return;
            }

            if (!mapProperties(req, request)) {
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
    @IgnorePermissionCheck
    @ApiOperation(value = "Set user properties once", request = SetUserProperties.class, response = Integer.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/set_properties_once")
    public void setPropertiesOnce(RakamHttpRequest request) {
        request.bodyHandler(s -> {
            SetUserProperties req;
            try {
                req = JsonHelper.readSafe(s, SetUserProperties.class);
            } catch (IOException e) {
                returnError(request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
                return;
            }

            if (!apiKeyService.checkPermission(req.project, WRITE_KEY, req.api.writeKey)) {
                returnError(request, UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
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
    @IgnorePermissionCheck
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/increment_property")
    @AllowCookie
    public JsonResponse incrementProperty(@ApiParam(name = "project") String project,
                                          @ApiParam(name = "api") User.UserContext api,
                                          @ApiParam(name = "user") String user,
                                          @ApiParam(name = "property") String property,
                                          @ApiParam("value") double value) {
        if (!apiKeyService.checkPermission(project, WRITE_KEY, api.writeKey)) {
            throw new RakamException(UNAUTHORIZED);
        }

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
    public void execute(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, PreCalculateQuery.class, query -> {
            String tableName = "_users_daily" +
                    Optional.ofNullable(query.collection).map(value -> "_" + value).orElse("") +
                    Optional.ofNullable(query.dimension).map(value -> "_by_" + value).orElse("");

            String name = "Daily users who did " +
                    Optional.ofNullable(query.collection).map(value -> " event " + value).orElse(" at least one event") +
                    Optional.ofNullable(query.dimension).map(value -> " grouped by " + value).orElse("");

            String table, dateColumn;
            if (query.collection == null) {
                table = String.format("SELECT cast(_time as date) as date, %s _user FROM _all",
                            Optional.ofNullable(query.dimension).map(v -> v + ",").orElse(""));
                dateColumn = "date";
            } else {
                table = query.collection;
                dateColumn = "cast(_time as date)";
            }

            String sqlQuery = String.format("SELECT %s as date, %s set(_user) _user_set FROM (%s) GROUP BY 1 %s",
                    dateColumn,
                    Optional.ofNullable(query.dimension).map(v -> v + " as dimension,").orElse(""), table,
                    Optional.ofNullable(query.dimension).map(v -> ", 2").orElse(""));

            return new DelegateQueryExecution(continuousQueryService.create(new ContinuousQuery(query.project, name, tableName, sqlQuery,
                    ImmutableList.of("date"), ImmutableMap.of()), query.replayHistoricalData), result -> {
                if (result.isFailed()) {
                    throw new RakamException("Failed to create continuous query: " + JsonHelper.encode(result.getError()), INTERNAL_SERVER_ERROR);
                }
                result.setProperty("preCalculated", new PreCalculatedTable(name, tableName));
                return result;
            });
        }, MASTER_KEY);
    }

    public static class PreCalculateQuery implements ProjectItem {
        public final String project;
        public final String collection;
        public final String dimension;
        public final boolean replayHistoricalData;

        public PreCalculateQuery(@ApiParam(name = "project") String project,
                                 @ApiParam(name = "collection", required = false) String collection,
                                 @ApiParam(name = "dimension", required = false) String dimension,
                                 @ApiParam(name = "replay_historical_data", required = false) Boolean replayHistoricalData) {
            this.project = project;
            this.collection = collection;
            this.dimension = dimension;
            this.replayHistoricalData = replayHistoricalData == null ? false : replayHistoricalData;
        }

        @Override
        public String project() {
            return project;
        }
    }

    public static class PreCalculatedTable {
        public final String name;
        public final String tableName;

        public PreCalculatedTable(String name, String tableName) {
            this.name = name;
            this.tableName = tableName;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Unset user property")
    @IgnorePermissionCheck
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "User does not exist.")})
    @Path("/unset_properties")
    @AllowCookie
    public JsonResponse unsetProperty(@ApiParam(name = "project") String project,
                                      @ApiParam(name = "api") User.UserContext api,
                                      @ApiParam(name = "user") String user,
                                      @ApiParam(name = "property") List<String> properties) {
        if (!apiKeyService.checkPermission(project, WRITE_KEY, api.writeKey)) {
            throw new RakamException(UNAUTHORIZED);
        }
        service.unsetProperties(project, user, properties);
        return JsonResponse.success();
    }

    public static class SetUserProperties {
        public final String project;
        public final String user;
        public final User.UserContext api;
        public final Map<String, Object> properties;

        @JsonCreator
        public SetUserProperties(@ApiParam(name = "project") String project,
                                 @ApiParam(name = "user") String user,
                                 @ApiParam(name = "api") User.UserContext api,
                                 @ApiParam(name = "properties") Map<String, Object> properties) {
            this.project = project;
            this.user = user;
            this.api = api;
            this.properties = properties;
        }
    }
}
