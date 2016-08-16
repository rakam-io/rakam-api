package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.collection.EventCollectionHttpService;
import org.rakam.collection.EventCollectionHttpService.HttpRequestParams;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.AbstractUserService.BatchUserOperationRequest;
import org.rakam.plugin.user.AbstractUserService.CollectionEvent;
import org.rakam.plugin.user.AbstractUserService.PreCalculateQuery;
import org.rakam.plugin.user.AbstractUserService.SingleUserBatchOperationRequest;
import org.rakam.plugin.user.UserStorage.Sorting;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpRequestException;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.AllowCookie;
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;
import org.rakam.util.RakamException;
import org.rakam.util.LogUtil;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_EXPOSE_HEADERS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.cookie.ServerCookieEncoder.STRICT;
import static java.lang.String.format;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.MASTER_KEY;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.collection.EventCollectionHttpService.getHeaderList;
import static org.rakam.collection.EventCollectionHttpService.setBrowser;
import static org.rakam.server.http.HttpServer.returnError;

@Path("/user")
@Api(value = "/user", nickname = "user", description = "User", tags = "user")
public class UserHttpService
        extends HttpService
{
    private final static Logger LOGGER = Logger.get(UserHttpService.class);
    private final byte[] OK_MESSAGE = "1".getBytes(UTF_8);

    private final UserPluginConfig config;
    private final static SqlParser sqlParser = new SqlParser();
    private final AbstractUserService service;
    private final Set<UserPropertyMapper> mappers;
    private final QueryHttpService queryService;
    private final ApiKeyService apiKeyService;

    @Inject
    public UserHttpService(UserPluginConfig config,
            Set<UserPropertyMapper> mappers,
            ApiKeyService apiKeyService,
            AbstractUserService service,
            QueryHttpService queryService)
    {
        this.service = service;
        this.config = config;
        this.apiKeyService = apiKeyService;
        this.queryService = queryService;
        this.mappers = mappers;
    }

    @ApiOperation(value = "Create new user", request = User.class, response = Integer.class)
    @Path("/create")
    @POST
    public void createUser(RakamHttpRequest request)
    {
        setPropertiesInline(request, (project, user) ->
                service.create(project, user.id, user.properties));
    }

    @JsonRequest
    @ApiOperation(value = "Create multiple new users", authorizations = @Authorization(value = "write_key"), notes = "Returns user ids. User id may be string or numeric.")
    @Path("/batch/create")
    public List<Object> createUsers(@Named("project") String project, @ApiParam("users") List<User> users)
    {
        try {
            return service.batchCreate(project, users);
        }
        catch (Exception e) {
            throw new RakamException(e.getMessage(), BAD_REQUEST);
        }
    }

    @GET
    @ApiOperation(value = "Get user storage metadata", authorizations = @Authorization(value = "read_key"))
    @JsonRequest
    @Path("/metadata")
    public MetadataResponse getMetadata(@Named("project") String project)
    {
        return new MetadataResponse(config.getIdentifierColumn(), service.getMetadata(project));
    }

    public static class MetadataResponse
    {
        public final List<SchemaField> columns;
        public final String identifierColumn;

        public MetadataResponse(String identifierColumn, List<SchemaField> columns)
        {
            this.identifierColumn = identifierColumn;
            this.columns = columns;
        }
    }

    public static Expression parseExpression(String filter)
    {
        if (filter != null) {
            try {
                synchronized (sqlParser) {
                    return sqlParser.createExpression(filter);
                }
            }
            catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", filter),
                        BAD_REQUEST);
            }
        }
        else {
            return null;
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
            @ApiParam(value = "limit", required = false) Integer limit)
    {
        Expression expression = parseExpression(filter);

        limit = limit == null ? 100 : Math.min(5000, limit);

        return service.searchUsers(project, columns, expression, event_filter, sorting, limit, offset);
    }

    @POST
    @JsonRequest
    @ApiOperation(value = "Get events of the user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/get_events")
    public CompletableFuture<List<CollectionEvent>> getEvents(@Named("project") String project,
            @ApiParam("user") String user,
            @ApiParam(value = "limit", required = false) Integer limit,
            @ApiParam(value = "properties", required = false) List<String> properties,
            @ApiParam(value = "offset", required = false) Instant offset)
    {
        return service.getEvents(project, user,
                properties == null ? Optional.empty() : Optional.of(properties),
                limit == null ? 15 : limit, offset);
    }

    @POST
    @JsonRequest
    @ApiOperation(value = "Get events of the user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/create_segment")
    public SuccessMessage createSegment(@Named("project") String project,
            @ApiParam("name") String name,
            @ApiParam("table_name") String tableName,
            @ApiParam(value = "filter_expression", required = false) String filterExpression,
            @ApiParam(value = "event_filters", required = false) List<UserStorage.EventFilter> eventFilters,
            @ApiParam("cache_eviction") Duration duration)
    {
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

        return SuccessMessage.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/get")
    public CompletableFuture<User> getUser(@Named("project") String project, @ApiParam("user") Object user)
    {
        return service.getUser(project, user);
    }

    public static class MergeRequest
    {
        public final Object id;
        public final User.UserContext api;
        public final Object anonymousId;
        public final long createdAt;
        public final long mergedAt;

        @JsonCreator
        public MergeRequest(@ApiParam("id") Object id,
                @ApiParam("api") User.UserContext api,
                @ApiParam("anonymous_id") Object anonymousId,
                @ApiParam("created_at") long createdAt,
                @ApiParam("merged_at") long mergedAt)
        {
            this.id = id;
            this.api = api;
            this.anonymousId = anonymousId;
            this.createdAt = createdAt;
            this.mergedAt = mergedAt;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Merge user with anonymous id", authorizations = @Authorization(value = "write_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/merge")
    @IgnoreApi
    @AllowCookie
    public void mergeUser(RakamHttpRequest request,
            @CookieParam("_anonymous_user") String anonymousIdFallback,
            @BodyParam MergeRequest mergeRequest)
    {
        // TODO: what if a user sends real user ids instead of its previous anonymous id?
        if (!config.getEnableUserMapping()) {
            throw new RakamException("The feature is not supported", PRECONDITION_FAILED);
        }

        Object anonymousId = mergeRequest.anonymousId;

        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
                Unpooled.wrappedBuffer(OK_MESSAGE));
        setBrowser(request, response);

        if (anonymousId == null) {
            if (anonymousIdFallback == null) {
                request.response(response).end();
                return;
            }

            anonymousId = anonymousIdFallback;
        }

        String project = apiKeyService.getProjectOfApiKey(mergeRequest.api.apiKey, WRITE_KEY);

        service.merge(project, mergeRequest.id, anonymousId,
                Instant.ofEpochMilli(mergeRequest.createdAt),
                Instant.ofEpochMilli(mergeRequest.mergedAt));
        request.response(response).end();
    }

    @ApiOperation(value = "Batch operation on a single user properties",
            request = SingleUserBatchOperationRequest.class, response = Integer.class,
            authorizations = {@Authorization(value = "write_key")})
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/batch")
    @JsonRequest
    public void batchSingleUserOperations(RakamHttpRequest request)
    {
        request.bodyHandler(s -> {
            SingleUserBatchOperationRequest req;
            try {
                req = JsonHelper.read(s, SingleUserBatchOperationRequest.class);
            }
            catch (Exception e) {
                returnError(request, e.getMessage(), BAD_REQUEST);
                return;
            }

            String project = apiKeyService.getProjectOfApiKey(req.api.apiKey, WRITE_KEY);

            InetAddress socketAddress = ((InetSocketAddress) request.context().channel()
                    .remoteAddress()).getAddress();

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));
            List<Cookie> cookies = mapEvent(mapper ->
                    mapper.map(project, req.data, new HttpRequestParams(request), socketAddress));

            service.batch(project, req.data);

            setBrowser(request, response);
            if (cookies != null && !cookies.isEmpty()) {
                response.headers().add(SET_COOKIE, STRICT.encode(cookies));
            }
            request.response(response).end();
        });
    }

    @ApiOperation(value = "Batch operations on user properties", request = BatchUserOperationRequest.class,
            response = Integer.class, authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/batch_operations")
    @JsonRequest
    public void batchUserOperations(RakamHttpRequest request)
    {
        request.bodyHandler(s -> {
            BatchUserOperationRequest req;
            try {
                req = JsonHelper.read(s, BatchUserOperationRequest.class);
            }
            catch (Exception e) {
                returnError(request, e.getMessage(), BAD_REQUEST);
                return;
            }

            String project = apiKeyService.getProjectOfApiKey(req.api.apiKey, MASTER_KEY);

            InetAddress socketAddress = ((InetSocketAddress) request.context().channel()
                    .remoteAddress()).getAddress();

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));
            List<Cookie> cookies = mapEvent(mapper ->
                    mapper.map(project, req.data, new HttpRequestParams(request), socketAddress));

            service.batch(project, req.data);

            setBrowser(request, response);
            if (cookies != null && !cookies.isEmpty()) {
                response.headers().add(SET_COOKIE, STRICT.encode(cookies));
            }
            request.response(response).end();
        });
    }

    public List<Cookie> mapEvent(Function<UserPropertyMapper, List<Cookie>> mapperFunction)
    {
        List<Cookie> cookies = null;
        for (UserPropertyMapper mapper : mappers) {
            // TODO: bound event mappers to Netty Channels and runStatementSafe them in separate thread
            List<Cookie> mapperCookies = mapperFunction.apply(mapper);
            if (mapperCookies != null) {
                if (cookies == null) {
                    cookies = new ArrayList<>();
                }
                cookies.addAll(mapperCookies);
            }
        }

        return cookies;
    }

    @ApiOperation(value = "Set user properties", request = User.class, response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/set_properties")
    @POST
    public void setProperties(RakamHttpRequest request)
    {
        setPropertiesInline(request, (project, user) -> service.setUserProperties(project, user.id, user.properties));
    }

    public void setPropertiesInline(RakamHttpRequest request, BiConsumer<String, User> mapper)
    {
        request.bodyHandler(s -> {
            User req;
            try {
                req = JsonHelper.readSafe(s, User.class);
            }
            catch (IOException e) {
                returnError(request, e.getMessage(), BAD_REQUEST);
                return;
            }

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
                    Unpooled.wrappedBuffer(OK_MESSAGE));
            setBrowser(request, response);

            try {
                String project = apiKeyService.getProjectOfApiKey(req.api.apiKey, WRITE_KEY);

                List<Cookie> cookies = mapProperties(project, req, request);
                if (cookies != null) {
                    response.headers().add(SET_COOKIE, STRICT.encode(cookies));
                }
                String headerList = getHeaderList(response.headers().iterator());
                if (headerList != null) {
                    response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
                }

                mapper.accept(project, req);
                request.response(response).end();
            }
            catch (RakamException e) {
                LogUtil.logException(request, e);
                EventCollectionHttpService.returnError(request, e.getMessage(), e.getStatusCode());
            }
            catch (HttpRequestException e) {
                EventCollectionHttpService.returnError(request, e.getMessage(), e.getStatusCode());
            }
            catch (Throwable t) {
                LOGGER.error(t);
                EventCollectionHttpService.returnError(request, "An error occurred", INTERNAL_SERVER_ERROR);
            }
        });
    }

    private List<Cookie> mapProperties(String project, User req, RakamHttpRequest request)
    {
        InetAddress socketAddress = ((InetSocketAddress) request.context().channel()
                .remoteAddress()).getAddress();

        List<Cookie> cookies = null;
        BatchUserOperationRequest op = new BatchUserOperationRequest(req.api,
                ImmutableList.of(new BatchUserOperationRequest.BatchUserOperations(req.id, req.properties, null, null, null, null)));

        for (UserPropertyMapper mapper : mappers) {
            try {
                List<Cookie> map = mapper.map(project, op.data, new HttpRequestParams(request), socketAddress);
                if (map != null) {
                    if (cookies == null) {
                        cookies = new ArrayList<>();
                    }

                    cookies.addAll(map);
                }
            }
            catch (Exception e) {
                LOGGER.error(e, "Error while mapping user properties in " + mapper.getClass().toString());
                return null;
            }
        }

        return cookies;
    }

    @JsonRequest
    @ApiOperation(value = "Set user properties once", request = User.class, response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/set_properties_once")
    public void setPropertiesOnce(RakamHttpRequest request)
    {
        request.bodyHandler(s -> {
            User req;
            try {
                req = JsonHelper.readSafe(s, User.class);
            }
            catch (IOException e) {
                returnError(request, e.getMessage(), BAD_REQUEST);
                return;
            }

            String project = apiKeyService.getProjectOfApiKey(req.api.apiKey, WRITE_KEY);

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));
            response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, request.headers().get(ORIGIN));

            List<Cookie> cookies = mapProperties(project, req, request);
            if (cookies != null) {
                response.headers().add(SET_COOKIE,
                        STRICT.encode(cookies));
            }
            String headerList = getHeaderList(response.headers().iterator());
            if (headerList != null) {
                response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
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
    public SuccessMessage incrementProperty(@ApiParam("api") User.UserContext api,
            @ApiParam("id") String user,
            @ApiParam("property") String property,
            @ApiParam("value") double value)
    {
        String project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
        service.incrementProperty(project, user, property, value);
        return SuccessMessage.success();
    }

    @ApiOperation(value = "Create pre-calculate rule",
            authorizations = @Authorization(value = "master_key")
    )
    @Consumes("text/event-stream")
    @IgnoreApi
    @GET
    @Path("/pre_calculate")
    public void precalculateUsers(RakamHttpRequest request)
    {
        queryService.handleServerSentQueryExecution(request, PreCalculateQuery.class,
                service::preCalculate, MASTER_KEY, false);
    }

    @JsonRequest
    @ApiOperation(value = "Unset user property")
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/unset_properties")
    @AllowCookie
    public SuccessMessage unsetProperty(@ApiParam("api") User.UserContext api,
            @ApiParam("id") Object id,
            @ApiParam("properties") List<String> properties)
    {
        String project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
        service.unsetProperties(project, id, properties);
        return SuccessMessage.success();
    }
}
