package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import org.apache.poi.ss.formula.functions.Even;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.collection.EventCollectionHttpService;
import org.rakam.collection.EventCollectionHttpService.HttpRequestParams;
import org.rakam.collection.SchemaField;
import org.rakam.module.website.UserIdEventMapper;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.user.AbstractUserService.CollectionEvent;
import org.rakam.plugin.user.AbstractUserService.PreCalculateQuery;
import org.rakam.plugin.user.UserPropertyMapper.BatchUserOperation;
import org.rakam.plugin.user.UserPropertyMapper.BatchUserOperation.Data;
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
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.AllowCookie;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;
import org.rakam.util.SentryUtil;

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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_EXPOSE_HEADERS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
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

    @JsonRequest
    @ApiOperation(value = "Create new user")
    @Path("/create")
    public Object createUser(@BodyParam User user)
    {
        String project = apiKeyService.getProjectOfApiKey(user.api != null ? user.api.apiKey : null, WRITE_KEY);

        return service.create(project, user.id, user.properties);
    }

    @JsonRequest
    @ApiOperation(value = "Create new users", authorizations = @Authorization(value = "write_key"))

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
            @ApiParam(value = "offset", required = false) Instant offset)
    {
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

        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get user", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/get")
    public CompletableFuture<org.rakam.plugin.user.User> getUser(@Named("project") String project,
            @ApiParam("user") Object user)
    {
        return service.getUser(project, user);
    }

    @JsonRequest
    @ApiOperation(value = "Merge user with anonymous id", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/merge")
    @AllowCookie
    public boolean mergeUser(@Named("project") String project,
            @ApiParam("user") String user,
            @ApiParam("api") User.UserContext api,
            @ApiParam("anonymous_id") String anonymousId,
            @ApiParam("created_at") Instant createdAt,
            @ApiParam("merged_at") Instant mergedAt)
    {
        // TODO: what if a user sends real user ids instead of its previous anonymous id?
        if (!config.getEnableUserMapping()) {
            throw new RakamException("The feature is not supported", HttpResponseStatus.PRECONDITION_FAILED);
        }
        service.merge(project, user, anonymousId, createdAt, mergedAt);
        return true;
    }

    @ApiOperation(value = "Batch operation user properties", request = User.class, response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 404, message = "User does not exist.")})
    @Path("/batch")
    @IgnoreApi
    @JsonRequest
    public void batchOperations(RakamHttpRequest request)
    {
        request.bodyHandler(s -> {
            BatchUserOperation req;
            try {
                req = JsonHelper.read(s, BatchUserOperation.class);
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
                    mapper.map(project, req, new HttpRequestParams(request), socketAddress));

            for (Data data : req.data) {
                if (data.setProperties != null) {
                    service.setUserProperties(project, req.id, data.setProperties);
                }
                if (data.setPropertiesOnce != null) {
                    service.setUserPropertiesOnce(project, req.id, data.setPropertiesOnce);
                }
                if (data.unsetProperties != null) {
                    service.unsetProperties(project, req.id, data.unsetProperties);
                }
                if (data.incrementProperties != null) {
                    for (Map.Entry<String, Double> entry : data.incrementProperties.entrySet()) {
                        service.incrementProperty(project, req.id, entry.getKey(), entry.getValue());
                    }
                }
            }

            setBrowser(request, response);
            if(cookies != null && !cookies.isEmpty()) {
                response.headers().add(SET_COOKIE, STRICT.encode(cookies));
            }
            request.response(response).end();
        });
    }


    public List<Cookie> mapEvent(Function<UserPropertyMapper, List<Cookie>> mapperFunction)
    {
        List<Cookie> cookies = null;
        for (UserPropertyMapper mapper : mappers) {
            // TODO: bound event mappers to Netty Channels and run them in separate thread
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
            EventCollectionHttpService.setBrowser(request, response);

            try {

                String project = apiKeyService.getProjectOfApiKey(req.api.apiKey, WRITE_KEY);

                List<Cookie> cookies = mapProperties(project, req, request);
                if (cookies != null) {
                    response.headers().add(SET_COOKIE,
                            STRICT.encode(cookies));
                }
                String headerList = getHeaderList(response.headers().iterator());
                if (headerList != null) {
                    response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
                }

                service.setUserProperties(project, req.id, req.properties);
                request.response(response).end();
            }
            catch (RakamException e) {
                SentryUtil.logException(request, e);
                EventCollectionHttpService.returnError(request, e.getMessage(), e.getStatusCode());
            }
            catch (HttpRequestException e) {
                EventCollectionHttpService.returnError(request, e.getMessage(), e.getStatusCode());
            } catch (Throwable t) {
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
        BatchUserOperation op = new BatchUserOperation(req.id, req.api, ImmutableList.of(new Data(req.properties, null, null, null, null)));
        req.id = op.id;

        for (UserPropertyMapper mapper : mappers) {
            try {
                List<Cookie> map = mapper.map(project, op, new HttpRequestParams(request), socketAddress);
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
    public JsonResponse incrementProperty(@ApiParam("api") User.UserContext api,
            @ApiParam("id") String user,
            @ApiParam("property") String property,
            @ApiParam("value") double value)
    {
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
    public void precalculateUsers(RakamHttpRequest request)
    {
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
            @ApiParam("properties") List<String> properties)
    {
        String project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
        service.unsetProperties(project, id, properties);
        return JsonResponse.success();
    }
}
