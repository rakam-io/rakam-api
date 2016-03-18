package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.util.CharsetUtil;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.EventStore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.SwaggerJacksonAnnotationIntrospector;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.util.IgnorePermissionCheck;
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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

@Path("/event")
@Api(value = "/event", nickname = "collectEvent", description = "Event collection module", tags = {"event"})
public class EventCollectionHttpService extends HttpService {
    final static Logger LOGGER = Logger.get(EventCollectionHttpService.class);
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final byte[] OK_MESSAGE = "1".getBytes(UTF_8);

    private final EventStore eventStore;
    private final Set<EventMapper> eventMappers;
    private final Metastore metastore;
    private final ApiKeyService apiKeyService;
    private final Set<EventProcessor> eventProcessors;

    @Inject
    public EventCollectionHttpService(EventStore eventStore, ApiKeyService apiKeyService,EventDeserializer deserializer, EventListDeserializer eventListDeserializer, Set<EventMapper> mappers, Set<EventProcessor> eventProcessors, Metastore metastore) {
        this.eventStore = eventStore;
        this.eventMappers = mappers;
        this.eventProcessors = eventProcessors;
        this.metastore = metastore;
        this.apiKeyService = apiKeyService;

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
    }

    private List<Cookie> mapEvent(Event event, HttpHeaders headers, InetAddress remoteAddress, DefaultFullHttpResponse response) {
        List<Cookie> responseAttachment = null;
        for (EventMapper mapper : eventMappers) {
            try {
                // TODO: bound event mappers to Netty Channels and run them in separate thread
                final List<Cookie> map = mapper.map(event, headers, remoteAddress, response);
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
                final List<Cookie> map = eventProcessor.map(event, headers, remoteAddress, response);
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
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));

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
                    DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, UNAUTHORIZED, byteBuf);
                    errResponse.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
                    request.response(errResponse).end();
                    return;
                }

                cookies = mapEvent(event, headers, getRemoteAddress(socketAddress), response);

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
                DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, INTERNAL_SERVER_ERROR, byteBuf);
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

        return apiKeyService.checkPermission(project, ApiKeyService.AccessKeyType.WRITE_KEY, writeKey);
    }

    @POST
    @ApiOperation(value = "Collect multiple events", request = EventList.class, response = Integer[].class,
            authorizations = @Authorization(value = "write_key")
    )
    @IgnorePermissionCheck
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/batch")
    public void batchEvents(RakamHttpRequest request) {
        HttpHeaders headers = request.headers();

        request.bodyHandler(buff -> {
            List<Cookie> entries = null;

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));

            try {
                EventList events = jsonMapper.readValue(buff, EventList.class);

                Event.EventContext context = events.api;
                if (context.checksum != null && !validateChecksum(request, context.checksum, buff)) {
                    return;
                }

                if (!validateProjectPermission(events.project, context.writeKey)) {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer("\"api key is invalid\"".getBytes(CharsetUtil.UTF_8));
                    DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, UNAUTHORIZED, byteBuf);
                    errResponse.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
                    request.response(errResponse).end();
                    return;
                }

                InetAddress remoteAddress = getRemoteAddress(request.getRemoteAddress());

                for (Event event : events.events) {
                    List<Cookie> mapperEntries = mapEvent(event, headers, remoteAddress, response);
                    if (mapperEntries != null) {
                        if (entries == null) {
                            entries = new ArrayList<>();
                        }
                        entries.addAll(mapperEntries);
                    }
                }

                try {
                    int[] ints = eventStore.storeBatch(events.events);
                } catch (Exception e) {
                    LOGGER.error(e, "error while storing event.");
                    request.response("0", BAD_REQUEST).end();
                }

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
                DefaultFullHttpResponse errResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, INTERNAL_SERVER_ERROR, byteBuf);
                errResponse.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
                request.response(errResponse).end();
                return;
            }

            if (entries != null) {
                response.headers().add(HttpHeaders.Names.SET_COOKIE,
                        ServerCookieEncoder.STRICT.encode(entries));
            }

            String headerList = getHeaderList(response.headers().iterator());
            if (headerList != null) {
                response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
            }

            response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");

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
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, INTERNAL_SERVER_ERROR, byteBuf);
            response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            request.response(response).end();
            return false;
        }

        if (!DatatypeConverter.printHexBinary(md.digest(expected.getBytes(UTF_8))).equals(checksum.toUpperCase(Locale.ENGLISH))) {
            ByteBuf byteBuf = Unpooled.wrappedBuffer("\"checksum is invalid\"".getBytes(CharsetUtil.UTF_8));
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, BAD_REQUEST, byteBuf);
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
}
