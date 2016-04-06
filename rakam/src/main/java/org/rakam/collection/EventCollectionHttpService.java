package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.util.CharsetUtil;
import org.rakam.analysis.ApiKeyService;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.EventStore;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.SwaggerJacksonAnnotationIntrospector;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;

@Path("/event")
@Api(value = "/event", nickname = "collectEvent", description = "Event collection module", tags = {"event"})
public class EventCollectionHttpService extends HttpService {
    final static Logger LOGGER = Logger.get(EventCollectionHttpService.class);
    private final ObjectMapper jsonMapper;
    private final ObjectMapper csvMapper;
    private final byte[] OK_MESSAGE = "1".getBytes(UTF_8);
    private final byte[] NOT_OK_MESSAGE = "0".getBytes(UTF_8);

    private final EventStore eventStore;
    private final Set<EventMapper> eventMappers;
    private final ApiKeyService apiKeyService;
    private final Set<EventProcessor> eventProcessors;
    private final CsvEventDeserializer csvEventDeserializer;
    private final AvroEventDeserializer avroEventDeserializer;

    @Inject
    public EventCollectionHttpService(EventStore eventStore, ApiKeyService apiKeyService,
                                      JsonEventDeserializer deserializer,
                                      AvroEventDeserializer avroEventDeserializer,
                                      EventListDeserializer eventListDeserializer,
                                      CsvEventDeserializer csvEventDeserializer,
                                      Set<EventMapper> mappers, Set<EventProcessor> eventProcessors) {
        this.eventStore = eventStore;
        this.eventMappers = mappers;
        this.csvEventDeserializer = csvEventDeserializer;
        this.eventProcessors = eventProcessors;
        this.apiKeyService = apiKeyService;

        jsonMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Event.class, deserializer);
        module.addDeserializer(EventList.class, eventListDeserializer);
        jsonMapper.registerModule(module);

        jsonMapper.registerModule(new SimpleModule("swagger", Version.unknownVersion()) {
            @Override
            public void setupModule(SetupContext context) {
                context.insertAnnotationIntrospector(new SwaggerJacksonAnnotationIntrospector());
            }
        });

        this.avroEventDeserializer = avroEventDeserializer;
        csvMapper = new CsvMapper();
        csvMapper.registerModule(new SimpleModule().addDeserializer(EventList.class, csvEventDeserializer));
    }

    private List<Cookie> mapEvent(Event event, HttpHeaders requestHeaders, InetAddress remoteAddress, HttpHeaders responseHeaders) {
        List<Cookie> responseAttachment = null;
        for (EventMapper mapper : eventMappers) {
            try {
                // TODO: bound event mappers to Netty Channels and run them in separate thread
                final List<Cookie> map = mapper.map(event, requestHeaders, remoteAddress);
                if (map != null) {
                    if (responseAttachment == null) {
                        responseAttachment = new ArrayList<>();
                    }

                    responseAttachment.addAll(map);
                }
            } catch (Exception e) {
                throw new RuntimeException("An error occurred while processing event in " + mapper.getClass().getName(), e);
            }
        }

        for (EventProcessor eventProcessor : eventProcessors) {
            try {
                final List<Cookie> map = eventProcessor.map(event, requestHeaders, remoteAddress, responseHeaders);
                if (map != null) {
                    if (responseAttachment == null) {
                        responseAttachment = new ArrayList<>();
                    }

                    responseAttachment.addAll(map);
                }
            } catch (Exception e) {
                throw new RuntimeException("An error occurred while processing event in " + eventProcessor.getClass().getName(), e);
            }
        }
        return responseAttachment;
    }

    @POST
    @IgnorePermissionCheck
    @ApiOperation(value = "Collect event", response = Integer.class, request = Event.class,
            authorizations = @Authorization(value = "write_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/collect")
    public void collectEvent(RakamHttpRequest request) {
        String socketAddress = request.getRemoteAddress();
        HttpHeaders headers = request.headers();

        request.bodyHandler(buff -> {
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));

            final List<Cookie> cookies;

            try {
                Event event = jsonMapper.readValue(buff, Event.class);

                Event.EventContext context = event.api();

                if (context == null) {
                    request.response("\"api key is missing\"", UNAUTHORIZED).end();
                    return;
                }

                if (context.checksum != null && !validateChecksum(request, context.checksum, buff)) {
                    return;
                }

                if (!validateProjectPermission(event.project(), context.writeKey)) {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer("\"api key is invalid\"".getBytes(CharsetUtil.UTF_8));
                    DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED, byteBuf);
                    errResponse.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
                    request.response(errResponse).end();
                    return;
                }


                cookies = mapEvent(event, headers, getRemoteAddress(socketAddress), response.trailingHeaders());

                eventStore.store(event);
            } catch (JsonMappingException e) {
                request.response("\"" + e.getMessage() + "\"", BAD_REQUEST).end();
                return;
            } catch (IOException e) {
                request.response("\"json couldn't parsed\"", BAD_REQUEST).end();
                return;
            } catch (RakamException e) {
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            } catch (Exception e) {
                LOGGER.error(e, "Error while collecting event");

                ByteBuf byteBuf = Unpooled.wrappedBuffer("\"internal server error\"".getBytes(CharsetUtil.UTF_8));
                DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, byteBuf);
                errResponse.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
                request.response(errResponse).end();
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

            request.response(response).end();
        });
    }

    private InetAddress getRemoteAddress(String socketAddress) {
        try {
            return InetAddress.getByName(socketAddress);
        } catch (UnknownHostException e) {
            return null;
        }
    }

    private boolean validateProjectPermission(String project, String writeKey) {
        if (writeKey == null) {
            return false;
        }

        return apiKeyService.checkPermission(project, WRITE_KEY, writeKey);
    }

    @POST
    @ApiOperation(value = "Send Bulk events", request = EventList.class, response = Integer.class,
            authorizations = @Authorization(value = "master_key")
    )
    @IgnorePermissionCheck
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."), @ApiResponse(code = 409, message = "The content is partially updated.")})
    @Path("/bulk")
    public void bulkEvents(RakamHttpRequest request) {
        storeEvents(request,
                buff -> {
                    String contentType = request.headers().get(CONTENT_TYPE);
                    if ("application/json".equals(contentType)) {
                        return jsonMapper.readValue(buff, EventList.class);
                    } else {
                        String project = getParam(request.params(), "project");
                        String collection = getParam(request.params(), "collection");
                        String api_key = getParam(request.params(), "api_key");

                        checkProject(project);
                        checkCollection(collection);
                        if (!apiKeyService.checkPermission(project, WRITE_KEY, api_key)) {
                            throw new RakamException(FORBIDDEN);
                        }

                        if ("application/avro".equals(contentType)) {
                            return avroEventDeserializer.deserialize(project, collection, api_key, buff);

                        } else if ("text/csv".equals(contentType)) {
                            return csvMapper.reader(EventList.class).with(ContextAttributes.getEmpty()
                                            .withSharedAttribute("project", project)
                                            .withSharedAttribute("collection", collection)
                                            .withSharedAttribute("api_key", api_key)
                            ).readValue(buff);
                        }
                    }

                    throw new RakamException("Unsupported content type: " + contentType, BAD_REQUEST);
                },
                (events, responseHeaders) -> {
                    try {
                        eventStore.storeBulk(events, !ImmutableList.of("false").equals(request.params().get("commit")));
                    } catch (Exception e) {
                        LOGGER.error(e, "error while storing event.");
                        return new HeaderDefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED,
                                Unpooled.wrappedBuffer(NOT_OK_MESSAGE), responseHeaders);
                    }

                    return new HeaderDefaultFullHttpResponse(HTTP_1_1, OK,
                            Unpooled.wrappedBuffer(OK_MESSAGE),
                            responseHeaders);
                });
    }

    private String getParam(Map<String, List<String>> params, String param) {
        List<String> strings = params.get(param);
        if (strings == null || strings.size() == 0) {
            throw new RakamException(String.format("%s query parameter is required", param), BAD_REQUEST);
        }

        return strings.get(strings.size() - 1);
    }

    @POST
    @ApiOperation(value = "Commit Bulk events", authorizations = @Authorization(value = "master_key"))
    @JsonRequest
    @Path("/bulk/commit")
    public CompletableFuture<QueryResult> commitBulkEvents(@ApiParam(name = "project") String project,
                                                           @ApiParam(name = "collection") String collection) {
        return eventStore.commit(project, collection);
    }

    @POST
    @ApiOperation(value = "Collect multiple events", request = EventList.class, response = Integer.class,
            authorizations = @Authorization(value = "write_key")
    )
    @IgnorePermissionCheck
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."), @ApiResponse(code = 409, message = "The content is partially updated.")})
    @Path("/batch")
    public void batchEvents(RakamHttpRequest request) {
        storeEvents(request, buff -> jsonMapper.readValue(buff, EventList.class),
                (events, responseHeaders) -> {
                    int[] errorIndexes;

                    try {
                        errorIndexes = eventStore.storeBatch(events);
                    } catch (Exception e) {
                        LOGGER.error(e, "error while storing event.");
                        return new HeaderDefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED,
                                Unpooled.wrappedBuffer(NOT_OK_MESSAGE), responseHeaders);
                    }

                    if (errorIndexes.length == 0) {
                        return new HeaderDefaultFullHttpResponse(HTTP_1_1, OK,
                                Unpooled.wrappedBuffer(OK_MESSAGE),
                                responseHeaders);
                    } else {
                        return new HeaderDefaultFullHttpResponse(HTTP_1_1, CONFLICT,
                                Unpooled.wrappedBuffer(JsonHelper.encodeAsBytes(errorIndexes)),
                                responseHeaders);
                    }
                });
    }

    public void storeEvents(RakamHttpRequest request, ThrowableFunction mapper, BiFunction<List<Event>, HttpHeaders, FullHttpResponse> responseFunction) {
        HttpHeaders headers = request.headers();

        request.bodyHandler(buff -> {
            List<Cookie> entries = null;

            DefaultHttpHeaders responseHeaders = new DefaultHttpHeaders();
            responseHeaders.set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");

            FullHttpResponse response;
            try {
                EventList events = mapper.apply(buff);

                Event.EventContext context = events.api;
                if (context.checksum != null && !validateChecksum(request, context.checksum, buff)) {
                    return;
                }

                if (!validateProjectPermission(events.project, context.writeKey)) {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer("\"api key is invalid\"".getBytes(CharsetUtil.UTF_8));
                    request.response(new HeaderDefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED, byteBuf, responseHeaders)).end();
                    return;
                }

                InetAddress remoteAddress = getRemoteAddress(request.getRemoteAddress());

                for (Event event : events.events) {
                    List<Cookie> mapperEntries = mapEvent(event, headers, remoteAddress, responseHeaders);
                    if (mapperEntries != null) {
                        if (entries == null) {
                            entries = new ArrayList<>();
                        }
                        entries.addAll(mapperEntries);
                    }
                }

                response = responseFunction.apply(events.events, responseHeaders);

            } catch (JsonMappingException e) {
                if (e.getCause() != null) {
                    request.response(e.getCause().getMessage(), BAD_REQUEST).end();
                    return;
                }
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            } catch (IOException e) {
                request.response("\"json couldn't parsed\"", BAD_REQUEST).end();
                return;
            } catch (RakamException e) {
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            } catch (Exception e) {
                LOGGER.error(e, "Error while collecting event");

                ByteBuf byteBuf = Unpooled.wrappedBuffer("0".getBytes(CharsetUtil.UTF_8));
                DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, byteBuf);
                errResponse.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
                request.response(errResponse).end();
                return;
            }

            if (entries != null) {
                responseHeaders.add(HttpHeaders.Names.SET_COOKIE,
                        ServerCookieEncoder.STRICT.encode(entries));
            }

            String headerList = getHeaderList(responseHeaders.iterator());
            if (headerList != null) {
                responseHeaders.set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
            }

            request.response(response).end();
        });
    }

    private String getHeaderList(Iterator<Map.Entry<String, String>> it) {
        StringBuilder builder = null;
        while (it.hasNext()) {
            String key = it.next().getKey();
            if (!key.equals(SET_COOKIE)) {
                if (builder == null) {
                    builder = new StringBuilder();
                }
                if (builder.length() != 0) {
                    builder.append(',');
                }
                builder.append(key);
            }
        }
        return builder == null ? null : builder.toString();
    }

    private boolean validateChecksum(RakamHttpRequest request, String checksum, String expected) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            ByteBuf byteBuf = Unpooled.wrappedBuffer("0".getBytes(CharsetUtil.UTF_8));
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, byteBuf);
            response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            request.response(response).end();
            return false;
        }

        if (!DatatypeConverter.printHexBinary(md.digest(expected.getBytes(UTF_8))).equals(checksum.toUpperCase(Locale.ENGLISH))) {
            ByteBuf byteBuf = Unpooled.wrappedBuffer("\"checksum is invalid\"".getBytes(CharsetUtil.UTF_8));
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST, byteBuf);
            response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            request.response(response).end();
            return false;
        }

        return true;
    }

    public static class EventList {
        public final Event.EventContext api;
        public final String project;
        public final List<Event> events;

        @JsonCreator
        public EventList(@ApiParam(name = "api") Event.EventContext api,
                         @ApiParam(name = "project") String project,
                         @ApiParam(name = "events") List<Event> events) {
            this.project = checkNotNull(project, "project parameter is null");
            this.events = checkNotNull(events, "events parameter is null");
            this.api = checkNotNull(api, "api is null");
        }
    }

    public static final class HeaderDefaultFullHttpResponse extends DefaultHttpResponse implements FullHttpResponse {
        private final ByteBuf content;
        private final HttpHeaders trailingHeaders;

        public HeaderDefaultFullHttpResponse(HttpVersion version, HttpResponseStatus status, ByteBuf content, HttpHeaders headers) {
            super(version, status);
            trailingHeaders = headers;
            this.content = content;
        }

        @Override
        public HttpHeaders trailingHeaders() {
            return trailingHeaders;
        }

        @Override
        public HttpHeaders headers() {
            return trailingHeaders;
        }

        @Override
        public ByteBuf content() {
            return content;
        }

        @Override
        public int refCnt() {
            return content.refCnt();
        }

        @Override
        public FullHttpResponse retain() {
            content.retain();
            return this;
        }

        @Override
        public FullHttpResponse retain(int increment) {
            content.retain(increment);
            return this;
        }

        @Override
        public boolean release() {
            return content.release();
        }

        @Override
        public boolean release(int decrement) {
            return content.release(decrement);
        }

        @Override
        public FullHttpResponse setProtocolVersion(HttpVersion version) {
            super.setProtocolVersion(version);
            return this;
        }

        @Override
        public FullHttpResponse setStatus(HttpResponseStatus status) {
            super.setStatus(status);
            return this;
        }

        @Override
        public FullHttpResponse copy() {
            DefaultFullHttpResponse copy = new DefaultFullHttpResponse(
                    getProtocolVersion(), getStatus(), content().copy(), true);
            copy.headers().set(headers());
            copy.trailingHeaders().set(trailingHeaders());
            return copy;
        }

        @Override
        public FullHttpResponse duplicate() {
            DefaultFullHttpResponse duplicate = new DefaultFullHttpResponse(getProtocolVersion(), getStatus(),
                    content().duplicate(), true);
            duplicate.headers().set(headers());
            duplicate.trailingHeaders().set(trailingHeaders());
            return duplicate;
        }
    }

    interface ThrowableFunction {
        EventList apply(String buffer) throws IOException, JsonParseException, JsonMappingException;
    }
}
