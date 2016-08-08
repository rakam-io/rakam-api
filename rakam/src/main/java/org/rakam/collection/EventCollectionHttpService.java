package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStore.CopyType;
import org.rakam.report.ChainQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.server.http.HttpRequestException;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.SwaggerJacksonAnnotationIntrospector;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
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
import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.ByteStreams.toByteArray;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.MASTER_KEY;
import static org.rakam.plugin.EventStore.CopyType.AVRO;
import static org.rakam.plugin.EventStore.CopyType.CSV;
import static org.rakam.plugin.EventStore.CopyType.JSON;
import static org.rakam.plugin.EventStore.SUCCESSFUL_BATCH;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.encodeAsBytes;
import static org.rakam.util.StandardErrors.CONFLICT_COMMIT_MESSAGE;
import static org.rakam.util.StandardErrors.PARTIAL_ERROR_MESSAGE;
import static org.rakam.util.ValidationUtil.checkCollection;

@Path("/event")
@Api(value = "/event", nickname = "collectEvent", description = "Event collection", tags = {"collect"})
public class EventCollectionHttpService
        extends HttpService
{
    private final static Logger LOGGER = Logger.get(EventCollectionHttpService.class);

    private final byte[] OK_MESSAGE = "1".getBytes(UTF_8);

    private final ObjectMapper jsonMapper;
    private final ObjectMapper csvMapper;
    private final EventStore eventStore;
    private final Set<EventMapper> eventMappers;
    private final ApiKeyService apiKeyService;
    private final AvroEventDeserializer avroEventDeserializer;
    private final Metastore metastore;
    private final QueryHttpService queryHttpService;

    @Inject
    public EventCollectionHttpService(EventStore eventStore, ApiKeyService apiKeyService,
            JsonEventDeserializer deserializer,
            QueryHttpService queryHttpService,
            AvroEventDeserializer avroEventDeserializer,
            EventListDeserializer eventListDeserializer,
            CsvEventDeserializer csvEventDeserializer,
            Metastore metastore,
            Set<EventMapper> mappers)
    {
        this.eventStore = eventStore;
        this.eventMappers = mappers;
        this.apiKeyService = apiKeyService;
        this.queryHttpService = queryHttpService;
        this.metastore = metastore;

        jsonMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Event.class, deserializer);
        module.addDeserializer(EventList.class, eventListDeserializer);
        jsonMapper.registerModule(module);

        jsonMapper.registerModule(new SimpleModule("swagger", Version.unknownVersion())
        {
            @Override
            public void setupModule(SetupContext context)
            {
                context.insertAnnotationIntrospector(new SwaggerJacksonAnnotationIntrospector());
            }
        });

        this.avroEventDeserializer = avroEventDeserializer;
        csvMapper = new CsvMapper();
        csvMapper.registerModule(new SimpleModule().addDeserializer(EventList.class, csvEventDeserializer));
    }

    public List<Cookie> mapEvent(Function<EventMapper, List<Cookie>> mapperFunction)
    {
        List<Cookie> cookies = null;
        for (EventMapper mapper : eventMappers) {
            // TODO: bound event mappers to Netty Channels and
            // runStatementSafe them in separate thread
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

    public static void returnError(RakamHttpRequest request, String msg, HttpResponseStatus status)
    {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(JsonHelper.encodeAsBytes(errorMessage(msg, status)));
        DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HTTP_1_1, status, byteBuf);
        setBrowser(request, errResponse);
        request.response(errResponse).end();
    }

    public static void setBrowser(HttpRequest request, HttpResponse response)
    {
        response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        if (request.headers().contains(ORIGIN)) {
            response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, request.headers().get(ORIGIN));
        }
        String headerList = getHeaderList(request.headers().iterator());
        if (headerList != null) {
            response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
        }
    }

    @POST
    @ApiOperation(value = "Collect event", response = Integer.class, request = Event.class)
    @Path("/collect")
    public void collectEvent(RakamHttpRequest request)
    {
        String socketAddress = request.getRemoteAddress();

        request.bodyHandler(buff -> {
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));

            final List<Cookie> cookies;

            try {
                Event event = jsonMapper.readValue(buff, Event.class);

                Event.EventContext context = event.api();

                if (context.checksum != null && !validateChecksum(request, context.checksum, buff)) {
                    return;
                }

                cookies = mapEvent((mapper) -> mapper.map(event, new HttpRequestParams(request),
                        getRemoteAddress(socketAddress), response.trailingHeaders()));

                eventStore.store(event);
            }
            catch (JsonMappingException e) {
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                returnError(request, "JSON couldn't parsed: " + message, BAD_REQUEST);
                return;
            }
            catch (IOException e) {
                returnError(request, "JSON couldn't parsed: " + e.getMessage(), BAD_REQUEST);
                return;
            }
            catch (RakamException e) {
                LogUtil.logException(request, e);
                returnError(request, e.getMessage(), e.getStatusCode());
                return;
            }
            catch (HttpRequestException e) {
                returnError(request, e.getMessage(), e.getStatusCode());
                return;
            }
            catch (IllegalArgumentException e) {
                LogUtil.logException(request, e);
                returnError(request, e.getMessage(), BAD_REQUEST);
                return;
            }
            catch (Exception e) {
                LOGGER.error(e, "Error while collecting event");

                returnError(request, "An error occurred", INTERNAL_SERVER_ERROR);
                return;
            }

            if (cookies != null) {
                response.headers().add(HttpHeaders.Names.SET_COOKIE,
                        ServerCookieEncoder.STRICT.encode(cookies));
            }
            String headerList = getHeaderList(response.headers().iterator());
            if (headerList != null) {
                response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
            }
            if (request.headers().contains(ORIGIN)) {
                response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, request.headers().get(ORIGIN));
            }

            request.response(response).end();
        });
    }

    private InetAddress getRemoteAddress(String socketAddress)
    {
        try {
            return InetAddress.getByName(socketAddress);
        }
        catch (UnknownHostException e) {
            return null;
        }
    }

    @POST
    @ApiOperation(value = "Collect Bulk events", request = EventList.class, response = SuccessMessage.class, notes = "Bulk API requires master_key as api key and designed to handle large value of data. " +
            "The endpoint also accepts application/avro and text/csv formats. You need need to set 'collection' and 'master_key' query parameters if the content-type is not application/json.")
    @Path("/bulk")
    public void bulkEvents(RakamHttpRequest request)
    {
        storeEventsSync(request,
                buff -> {
                    String contentType = request.headers().get(CONTENT_TYPE);
                    if (contentType == null || "application/json".equals(contentType)) {
                        return jsonMapper.reader(EventList.class)
                                .with(ContextAttributes.getEmpty().withSharedAttribute("apiKey", MASTER_KEY))
                                .readValue(buff);
                    }
                    else {
                        if ("application/avro".equals(contentType)) {
                            String apiKey = getParam(request.params(), MASTER_KEY.getKey());
                            String project = apiKeyService.getProjectOfApiKey(apiKey, MASTER_KEY);
                            String collection = getParam(request.params(), "collection");

                            return avroEventDeserializer.deserialize(project, collection, utf8Slice(buff));
                        }
                        else if ("text/csv".equals(contentType)) {
                            String apiKey = getParam(request.params(), MASTER_KEY.getKey());
                            String project = apiKeyService.getProjectOfApiKey(apiKey, MASTER_KEY);
                            String collection = getParam(request.params(), "collection");

                            return csvMapper.reader(EventList.class)
                                    .with(ContextAttributes.getEmpty()
                                            .withSharedAttribute("project", project)
                                            .withSharedAttribute("collection", collection)
                                            .withSharedAttribute("apiKey", apiKey)
                                    ).readValue(buff);
                        }
                    }

                    throw new RakamException("Unsupported content type: " + contentType, BAD_REQUEST);
                },
                (events, responseHeaders) -> {
                    try {
                        eventStore.storeBulk(events);
                    }
                    catch (Exception e) {
                        List<Event> sample = events.size() > 5 ? events.subList(0, 5) : events;
                        LOGGER.error(new RuntimeException("Error executing EventStore bulk method: " + sample, e),
                                "Error while storing event.");

                        return new HeaderDefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR,
                                Unpooled.wrappedBuffer(encodeAsBytes(errorMessage("An error occurred", INTERNAL_SERVER_ERROR))),
                                responseHeaders);
                    }

                    return new HeaderDefaultFullHttpResponse(HTTP_1_1, OK,
                            Unpooled.wrappedBuffer(encodeAsBytes(SuccessMessage.success())),
                            responseHeaders);
                });
    }

    public static class BulkEventRemote
    {
        public final String collection;
        public final List<URL> urls;
        public final CopyType type;
        public final EventStore.CompressionType compression;
        public final Map<String, String> options;

        @JsonCreator
        public BulkEventRemote(@ApiParam("collection") String collection,
                @ApiParam("urls") List<URL> urls,
                @ApiParam("type") CopyType type,
                @ApiParam("compression") EventStore.CompressionType compression,
                @ApiParam("options") Map<String, String> options)
        {
            this.collection = collection;
            this.urls = urls;
            this.type = type;
            this.compression = compression;
            this.options = options;
        }
    }

    @GET
    @Consumes("text/event-stream")
    @IgnoreApi
    @ApiOperation(value = "Copy events from remote", request = BulkEventRemote.class, response = Integer.class)
    @Path("/copy")
    public void copyEventsRemote(RakamHttpRequest request)
            throws IOException
    {
        queryHttpService.handleServerSentQueryExecution(request, BulkEventRemote.class, (project, convert) ->
                eventStore.copy(project, convert.collection, convert.urls, convert.type, convert.compression, convert.options), MASTER_KEY, false);
    }

    @POST
    @ApiOperation(value = "Collect bulk events from remote", request = BulkEventRemote.class, response = Integer.class)
    @ApiResponses(value = {@ApiResponse(code = 409, message = PARTIAL_ERROR_MESSAGE, response = int[].class)})
    @Path("/bulk/remote")
    public void bulkEventsRemote(RakamHttpRequest request)
            throws IOException
    {
        storeEventsSync(request,
                buff -> {
                    BulkEventRemote query = JsonHelper.read(buff, BulkEventRemote.class);
                    String masterKey = request.headers().get("master_key");
                    String project = apiKeyService.getProjectOfApiKey(masterKey, MASTER_KEY);

                    checkCollection(query.collection);

                    if (query.urls.size() != 1) {
                        throw new RakamException("Only one url is supported", BAD_REQUEST);
                    }
                    if (query.compression != null) {
                        throw new RakamException("Compression is not supported yet", BAD_REQUEST);
                    }

                    URL url = query.urls.get(0);
                    if (query.type == JSON) {
                        return jsonMapper.readValue(url, EventList.class);
                    }
                    else if (query.type == CSV) {
                        return csvMapper.reader(EventList.class).with(ContextAttributes.getEmpty()
                                .withSharedAttribute("project", project)
                                .withSharedAttribute("collection", query.collection)
                                .withSharedAttribute("apiKey", masterKey)
                        ).readValue(url);
                    }
                    else if (query.type == AVRO) {
                        URLConnection conn = url.openConnection();
                        conn.setConnectTimeout(5000);
                        conn.setReadTimeout(5000);
                        conn.connect();

                        Slice slice = wrappedBuffer(toByteArray(conn.getInputStream()));
                        return avroEventDeserializer.deserialize(project, query.collection, slice);
                    }

                    throw new RakamException("Unsupported or missing type.", BAD_REQUEST);
                },
                (events, responseHeaders) -> {
                    try {
                        eventStore.storeBulk(events);
                    }
                    catch (Exception e) {
                        List<Event> sample = events.size() > 5 ? events.subList(0, 5) : events;
                        LOGGER.error(new RuntimeException("Error executing EventStore bulkRemote method: " + sample, e),
                                "Error while storing event.");
                        return new HeaderDefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR,
                                Unpooled.wrappedBuffer(encodeAsBytes(errorMessage("An error occurred", INTERNAL_SERVER_ERROR))),
                                responseHeaders);
                    }

                    return new HeaderDefaultFullHttpResponse(HTTP_1_1, OK,
                            Unpooled.wrappedBuffer(OK_MESSAGE),
                            responseHeaders);
                });
    }

    private String getParam(Map<String, List<String>> params, String param)
    {
        List<String> strings = params.get(param);
        if (strings == null || strings.size() == 0) {
            throw new RakamException(String.format("%s query parameter is required", param), BAD_REQUEST);
        }

        return strings.get(strings.size() - 1);
    }

    public static class CommitRequest
    {
        public final List<String> collections;

        @JsonCreator
        public CommitRequest(@ApiParam("collections") List<String> collections)
        {
            this.collections = collections;
        }
    }

    @ApiOperation(value = "Commit Bulk events", authorizations = @Authorization(value = "master_key"))
    @Path("/bulk/commit")
    @ApiResponses(value = {@ApiResponse(code = 409, message = CONFLICT_COMMIT_MESSAGE)})
    @JsonRequest
    public SuccessMessage commitBulkEvents(@Named("project") String project, @BodyParam CommitRequest request)
    {
        commitInternal(project, request);
        return SuccessMessage.success("Commit process started.");
    }

    @GET
    @Consumes("text/event-stream")
    @IgnoreApi
    @ApiOperation(value = "Commit Bulk events", request = CommitRequest.class, authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {@ApiResponse(code = 409, message = CONFLICT_COMMIT_MESSAGE)})
    @Path("/bulk/commit")
    public void commitBulkEventsStream(RakamHttpRequest request)
    {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", BAD_REQUEST))).end();
            return;
        }

        List<String> apiKey = request.params().get(MASTER_KEY.getKey());
        if (apiKey == null || data.isEmpty()) {
            String message = MASTER_KEY.getKey() + " query parameter is required";
            response.send("result", encode(errorMessage(message, BAD_REQUEST))).end();
            return;
        }

        String project;
        try {
            project = apiKeyService.getProjectOfApiKey(apiKey.get(0), MASTER_KEY);
        }
        catch (RakamException e) {
            response.send("result", encode(errorMessage(e.getMessage(), e.getStatusCode()))).end();
            return;
        }

        CommitRequest commitRequest = JsonHelper.read(data.get(0), CommitRequest.class);

        List<QueryExecution> queryExecutions;
        try {
            queryExecutions = commitInternal(project, commitRequest);
        }
        catch (UnsupportedOperationException e) {
            response.send("result", encode(errorMessage("Commit feature is not supported but this event store. /bulk endpoint commits automatically.", PRECONDITION_FAILED))).end();
            return;
        }

        queryHttpService.handleServerSentQueryExecution(request, new ChainQueryExecution(queryExecutions, null), false);
    }

    private List<QueryExecution> commitInternal(String project, CommitRequest commitRequest)
            throws UnsupportedOperationException
    {
        Collection<String> collections;
        if (commitRequest != null && commitRequest.collections != null && !commitRequest.collections.isEmpty()) {
            collections = commitRequest.collections;
        }
        else {
            collections = metastore.getCollectionNames(project);
        }

        ImmutableList.Builder<QueryExecution> builder = ImmutableList.builder();
        QueryExecution execution;
        for (String collection : collections) {
            execution = eventStore.commit(project, collection);
            builder.add(execution);
        }

        return builder.build();
    }

    private static final int[] FAILED_SINGLE_EVENT = new int[] {0};

    @POST
    @ApiOperation(notes = "Returns 1 if the events are collected.", value = "Collect multiple events", request = EventList.class, response = Integer.class)
    @ApiResponses(value = {
            @ApiResponse(code = 409, message = PARTIAL_ERROR_MESSAGE, response = int[].class)
    })
    @Path("/batch")
    public void batchEvents(RakamHttpRequest request)
    {
        storeEvents(request, buff -> jsonMapper.readValue(buff, EventList.class),
                (events, responseHeaders) -> {
                    CompletableFuture<int[]> errorIndexes;

                    if (events.size() > 0) {
                        boolean single = events.size() == 1;
                        try {
                            if (single) {
                                errorIndexes = EventStore.COMPLETED_FUTURE_BATCH;
                                if (events.size() == 1) {
                                    errorIndexes = eventStore.storeAsync(events.get(0))
                                            .handle((result, ex) -> (ex != null) ? FAILED_SINGLE_EVENT : SUCCESSFUL_BATCH);
                                }
                            }
                            else {
                                errorIndexes = eventStore.storeBatchAsync(events);
                            }
                        }
                        catch (Exception e) {
                            List<Event> sample = events.size() > 5 ? events.subList(0, 5) : events;
                            LOGGER.error(new RuntimeException("Error executing EventStore " + (single ? "store" : "batch") + " method: " + sample, e),
                                    "Error while storing event.");
                            return completedFuture(new HeaderDefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR,
                                    Unpooled.wrappedBuffer(encodeAsBytes(errorMessage("An error occurred", INTERNAL_SERVER_ERROR))),
                                    responseHeaders));
                        }
                    }
                    else {
                        errorIndexes = EventStore.COMPLETED_FUTURE_BATCH;
                    }

                    return errorIndexes.thenApply(result -> {
                        if (result.length == 0) {
                            return new HeaderDefaultFullHttpResponse(HTTP_1_1, OK,
                                    Unpooled.wrappedBuffer(OK_MESSAGE), responseHeaders);
                        }
                        else {
                            return new HeaderDefaultFullHttpResponse(HTTP_1_1, CONFLICT,
                                    Unpooled.wrappedBuffer(encodeAsBytes(result)), responseHeaders);
                        }
                    });
                });
    }

    public void storeEventsSync(RakamHttpRequest request, ThrowableFunction mapper, BiFunction<List<Event>, HttpHeaders, FullHttpResponse> responseFunction)
    {
        storeEvents(request, mapper,
                (events, entries) -> completedFuture(responseFunction.apply(events, entries)));
    }

    public void storeEvents(RakamHttpRequest request, ThrowableFunction mapper, BiFunction<List<Event>, HttpHeaders, CompletableFuture<FullHttpResponse>> responseFunction)
    {
        request.bodyHandler(buff -> {
            DefaultHttpHeaders responseHeaders = new DefaultHttpHeaders();
            responseHeaders.set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            if (request.headers().contains(ORIGIN)) {
                responseHeaders.set(ACCESS_CONTROL_ALLOW_ORIGIN, request.headers().get(ORIGIN));
            }

            CompletableFuture<FullHttpResponse> response;
            List<Cookie> entries;
            try {
                EventList events = mapper.apply(buff);

                Event.EventContext context = events.api;
                if (context.checksum != null && !validateChecksum(request, context.checksum, buff)) {
                    return;
                }

                InetAddress remoteAddress = getRemoteAddress(request.getRemoteAddress());

                entries = mapEvent((m) -> m.map(events, new HttpRequestParams(request),
                        remoteAddress, responseHeaders));

                response = responseFunction.apply(events.events, responseHeaders);
            }
            catch (JsonMappingException e) {
                returnError(request, "JSON couldn't parsed: " + e.getOriginalMessage(), BAD_REQUEST);
                return;
            }
            catch (IOException e) {
                returnError(request, "JSON couldn't parsed: " + e.getMessage(), BAD_REQUEST);
                return;
            }
            catch (RakamException e) {
                LogUtil.logException(request, e);
                returnError(request, e.getMessage(), e.getStatusCode());
                return;
            }
            catch (HttpRequestException e) {
                returnError(request, e.getMessage(), e.getStatusCode());
                return;
            }
            catch (IllegalArgumentException e) {
                LogUtil.logException(request, e);
                returnError(request, e.getMessage(), BAD_REQUEST);
                return;
            }
            catch (Exception e) {
                LOGGER.error(e, "Error while collecting event");

                returnError(request, "An error occurred", INTERNAL_SERVER_ERROR);
                return;
            }

            if (entries != null) {
                responseHeaders.add(HttpHeaders.Names.SET_COOKIE, ServerCookieEncoder.STRICT.encode(entries));
            }

            String headerList = getHeaderList(responseHeaders.iterator());
            if (headerList != null) {
                responseHeaders.set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
            }

            responseHeaders.add(CONTENT_TYPE, "application/json");

            response.thenAccept(resp -> request.response(resp).end());
        });
    }

    public static String getHeaderList(Iterator<Map.Entry<String, String>> it)
    {
        StringBuilder builder = new StringBuilder("cf-ray,server,status");
        while (it.hasNext()) {
            String key = it.next().getKey();
            if (!key.equals(SET_COOKIE)) {
                if (builder.length() != 0) {
                    builder.append(',');
                }
                builder.append(key.toLowerCase(Locale.ENGLISH));
            }
        }
        return builder == null ? null : builder.toString();
    }

    private boolean validateChecksum(RakamHttpRequest request, String checksum, String expected)
    {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e) {
            returnError(request, "0", INTERNAL_SERVER_ERROR);
            return false;
        }

        if (!DatatypeConverter.printHexBinary(md.digest(expected.getBytes(UTF_8))).equals(checksum.toUpperCase(Locale.ENGLISH))) {
            returnError(request, "Checksum is invalid", BAD_REQUEST);
            return false;
        }

        return true;
    }

    interface ThrowableFunction
    {
        EventList apply(String buffer)
                throws IOException;
    }

    public static class HttpRequestParams
            implements EventMapper.RequestParams
    {
        private final RakamHttpRequest request;

        public HttpRequestParams(RakamHttpRequest request)
        {
            this.request = request;
        }

        @Override
        public Collection<Cookie> cookies()
        {
            return request.cookies();
        }

        @Override
        public HttpHeaders headers()
        {
            return request.headers();
        }
    }
}
