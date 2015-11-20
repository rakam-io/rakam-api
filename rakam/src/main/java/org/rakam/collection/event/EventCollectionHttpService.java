package org.rakam.collection.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.log.Logger;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import org.rakam.collection.Event;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventMapper;
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
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableMap.of;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.rakam.collection.event.metastore.Metastore.AccessKeyType.WRITE_KEY;
import static org.rakam.util.JsonHelper.encode;

@Path("/event")
@Api(value = "/event", description = "Event collection module", tags = {"event"})
public class EventCollectionHttpService extends HttpService {
    final static Logger LOGGER = Logger.get(EventCollectionHttpService.class);
    private final ObjectMapper jsonMapper = new ObjectMapper(new EventParserJsonFactory());
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private final byte[] OK_MESSAGE = "1".getBytes(UTF8_CHARSET);

    private final EventStore eventStore;
    private final Set<EventMapper> mappers;
    private final Metastore metastore;

    @Inject
    public EventCollectionHttpService(EventStore eventStore, EventDeserializer deserializer, EventListDeserializer eventListDeserializer,  Set<EventMapper> mappers, Metastore metastore) {
        this.eventStore = eventStore;
        this.mappers = mappers;
        this.metastore = metastore;

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

    private List<Cookie> mapEvent(Event event, HttpHeaders headers, InetAddress socketAddress, DefaultFullHttpResponse response) {
        List<Cookie> responseAttachment = null;
        for (EventMapper mapper : mappers) {
            try {
                // TODO: bound event mappers to Netty Channels and run them in separate thread
                final List<Cookie> map = mapper.map(event, headers, socketAddress, response);
                if(map != null) {
                    if(responseAttachment == null) {
                        responseAttachment = new ArrayList<>();
                    }

                    responseAttachment.addAll(map);
                }
            } catch (Exception e) {
                throw new RuntimeException("An error occurred while processing event in " + mapper.getClass().getName(), e);
            }
        }
        return responseAttachment;
    }

    @POST
    @ApiOperation(value = "Collect event", response = Integer.class, request = Event.class,
            authorizations = @Authorization(value = "write_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/collect")
    public void collectEvent(RakamHttpRequest request) {

        InetSocketAddress socketAddress = (InetSocketAddress) request.context().channel()
                .remoteAddress();
        HttpHeaders headers = request.headers();
//        String checksum = headers.get("Content-MD5");


        request.bodyHandler(buff -> {
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));

            final List<Cookie> cookies;

            try {
                Event event = jsonMapper.readValue(buff, Event.class);

                Event.EventContext context = event.context();

                if(context == null) {
                    request.response("\"api key is missing\"", UNAUTHORIZED).end();
                    return;
                }

                if (context.checksum != null && !validateChecksum(request, context.checksum, buff)) {
                    return;
                }

                if(validateProjectPermission(event.project(), context.writeKey)) {
                    request.response("\"api key is invalid\"", UNAUTHORIZED).end();
                    return;
                }

                try {
                    cookies = mapEvent(event, headers, socketAddress.getAddress(), response);
                } catch (Exception e) {
                    LOGGER.error(e);
                    request.response("0", BAD_REQUEST).end();
                    return;
                }

                try {
                    eventStore.store(event);
                } catch (Exception e) {
                    LOGGER.error(e, "error while storing event.");
                    request.response("0", BAD_REQUEST).end();
                    return;
                }
            } catch (JsonMappingException e) {
                request.response("\""+e.getMessage()+"\"", BAD_REQUEST).end();
                return;
            } catch (IOException e) {
                request.response("\"json couldn't parsed\"", BAD_REQUEST).end();
                return;
            } catch (RakamException e) {
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            } catch (Exception e) {
                LOGGER.error(e, "Error while collecting event");
                request.response("\"internal server error\"", INTERNAL_SERVER_ERROR).end();
                return;
            }

            if(cookies != null) {
                response.headers().add(HttpHeaders.Names.SET_COOKIE,
                        ServerCookieEncoder.STRICT.encode(cookies));
            }
            String headerList = getHeaderList(response.headers().iterator());
            if(headerList != null) {
                response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
            }

            request.response(response).end();
        });
    }

    private boolean validateProjectPermission(String project, String writeKey) {
        if(writeKey == null) {
            return false;
        }

        return metastore.checkPermission(project, WRITE_KEY, writeKey);
    }

    @POST
    @ApiOperation(value = "Collect multiple events", request = EventList.class, response = Integer.class,
            authorizations = @Authorization(value = "write_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/batch")
    public void batchEvents(RakamHttpRequest request) {

        InetSocketAddress socketAddress = (InetSocketAddress) request.context().channel()
                .remoteAddress();
        HttpHeaders headers = request.headers();

        request.bodyHandler(buff -> {
            List<Cookie> entries = null;

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, Unpooled.wrappedBuffer(OK_MESSAGE));

            try {
                EventList events = jsonMapper.readValue(buff, EventList.class);

                Event.EventContext context = events.context;
                if (context.checksum != null && !validateChecksum(request, context.checksum, buff)) {
                    return;
                }

                if(!validateProjectPermission(events.project, context.writeKey)) {
                    request.response("\"api key is invalid\"", UNAUTHORIZED).end();
                    return;
                }

                if(events.events.size() > 0) {
                    for (int i = 1; i < events.events.size(); i++) {
                        if(!events.events.get(i).project().equals(events.project)) {
                            request.response("\"all events must belong to same project. try inserting events one by one.\"", UNAUTHORIZED).end();
                        }
                    }
                }


                for (Event event : events.events) {
                    try {
                        List<Cookie> mapperEntries = mapEvent(event, headers, socketAddress.getAddress(), response);
                        if(mapperEntries != null) {
                            if(entries == null) {
                                entries = new ArrayList<>();
                            }
                            entries.addAll(mapperEntries);
                        }
                    } catch (Exception e) {
                        LOGGER.error(e);
                        request.response("0", BAD_REQUEST).end();
                        return;
                    }
                }

                try {
                    eventStore.storeBatch(events.events);
                } catch (Exception e) {
                    LOGGER.error(e, "error while storing event.");
                    request.response("0", BAD_REQUEST).end();
                }

            } catch (JsonMappingException e) {
                if(e.getCause() != null) {
                    request.response(e.getCause().getMessage(), BAD_REQUEST).end();
                    return;
                }
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            } catch (IOException e) {
                request.response("json couldn't parsed", BAD_REQUEST).end();
                return;
            } catch (RakamException e) {
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            } catch (Exception e) {
                LOGGER.error(e, "Error while collecting event");
                request.response(e.getMessage(), INTERNAL_SERVER_ERROR).end();
                return;
            }

            if(entries != null) {
                response.headers().add(HttpHeaders.Names.SET_COOKIE,
                        ServerCookieEncoder.STRICT.encode(entries));
            }

            String headerList = getHeaderList(response.headers().iterator());
            if(headerList != null) {
                response.headers().set(ACCESS_CONTROL_EXPOSE_HEADERS, headerList);
            }

            response.headers().set(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");

            request.response(response).end();
        });
    }

    private String getHeaderList(Iterator<Map.Entry<String, String>> it) {
        StringBuilder builder = null;
        while(it.hasNext()) {
            String key = it.next().getKey();
            if(!key.equals(SET_COOKIE)) {
                if(builder == null) {
                    builder = new StringBuilder();
                }
                if(builder.length() != 0) {
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
            request.response("Internal Error", INTERNAL_SERVER_ERROR).end();
            return false;
        }

        if (!DatatypeConverter.printHexBinary(md.digest(expected.getBytes(UTF8_CHARSET))).equals(checksum.toUpperCase(Locale.ENGLISH))) {
            request.response(encode(of("error", "checksum is invalid")), BAD_REQUEST).end();
            return false;
        }

        return true;
    }

    public static class EventParserJsonFactory extends JsonFactory {
        @Override
        protected JsonParser _createParser(char[] data, int offset, int len, IOContext ctxt, boolean recyclable) throws IOException {
            return new EventDeserializer.SaveableReaderBasedJsonParser(ctxt, _parserFeatures, null, _objectCodec,
                    _rootCharSymbols.makeChild(_factoryFeatures),
                    data, offset, offset + len, recyclable);
        }
    }

    public static class EventList {
        public final Event.EventContext context;
        public final String project;
        public final List<Event> events;

        @JsonCreator
        public EventList(@ApiParam(name = "api") Event.EventContext context,
                         @ApiParam(name = "project") String project,
                         @ApiParam(name = "events") List<Event> events) {
            this.project = checkNotNull(project, "project parameter is null");
            this.events = checkNotNull(events, "events parameter is null");
            this.context = checkNotNull(context, "api is null");
        }
    }
}
