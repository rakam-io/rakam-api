package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventStore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiImplicitParam;
import org.rakam.server.http.annotations.ApiImplicitParams;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.of;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.rakam.util.JsonHelper.encode;

@Path("/event")
@Api(value = "/event", description = "Event collection module", tags = {"event", "collection"})
public class EventCollectionHttpService extends HttpService {
    final static Logger LOGGER = Logger.get(EventCollectionHttpService.class);
    private final ObjectMapper jsonMapper = new ObjectMapper(new EventParserJsonFactory());
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private final EventStore eventStore;
    private final Set<EventMapper> mappers;

    @Inject
    public EventCollectionHttpService(EventStore eventStore, EventDeserializer deserializer, Set<EventMapper> mappers) {
        this.eventStore = eventStore;
        this.mappers = mappers;

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Event.class, deserializer);
        jsonMapper.registerModule(module);
    }

    private boolean processEvent(Event event, HttpHeaders headers, InetAddress socketAddress) {
        for (EventMapper mapper : mappers) {
            try {
                // TODO: bound event mappers to Netty Channels and run them in separate thread
                mapper.map(event, headers, socketAddress);
            } catch (Exception e) {
                LOGGER.error(e, "An error occurred while processing event in "+mapper.getClass().getName());
                return false;
            }
        }

        try {
            eventStore.store(event);
        } catch (Exception e) {
            LOGGER.error(e, "error while storing event.");
            return false;
        }

        return true;
    }

    @POST
    @ApiOperation(value = "Collect event",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/collect")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "project", value = "The project id of the event", required = true, dataType = "string", paramType = "formData"),
            @ApiImplicitParam(name = "collection", value = "Collection of the event", required = true, dataType = "string", paramType = "formData"),
            @ApiImplicitParam(name = "properties", value = "Event properties", required = true, dataType = "object", paramType = "formData")
    })
    public void collect(RakamHttpRequest request) {

        InetSocketAddress socketAddress = (InetSocketAddress) request.context().channel()
                .remoteAddress();
        HttpHeaders headers = request.headers();
        String checksum = headers.get("Content-MD5");

        request.bodyHandler(buff -> {
            if(checksum != null) {
                MessageDigest md;
                try {
                    md = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    request.response("Internal Error", INTERNAL_SERVER_ERROR).end();
                    return;
                }
                if(!md.digest(checksum.getBytes(UTF8_CHARSET))
                        .equals(buff.getBytes(UTF8_CHARSET))) {
                    request.response(encode(of("error", "checksum is invalid")),
                            BAD_REQUEST).end();
                    return;
                }
            }

            boolean eventProcessed;

            try {
                // a trick to identify the type of the json data.
                if(buff.charAt(0) != '[') {
                    Event event = jsonMapper.readValue(buff, Event.class);
                    eventProcessed = processEvent(event, headers, socketAddress.getAddress());
                } else {
                    List<Event> events = jsonMapper.readValue(buff, List.class);
                    eventProcessed = true;
                    for (Event event : events) {
                        eventProcessed &= processEvent(event, headers, socketAddress.getAddress());
                    }
                }
            } catch (JsonMappingException e) {
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            } catch (IOException e) {
                request.response("json couldn't parsed", BAD_REQUEST).end();
                return;
            } catch (RakamException e) {
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            }catch (Exception e) {
                LOGGER.error(e, "Error while collecting event");
                request.response(e.getMessage(), INTERNAL_SERVER_ERROR).end();
                return;
            }

            request.response(eventProcessed ? "1" : "0", eventProcessed ? OK : BAD_GATEWAY).end();
        });
    }

    public static class EventParserJsonFactory extends JsonFactory {

        @Override
        protected JsonParser _createParser(char[] data, int offset, int len, IOContext ctxt,
                                           boolean recyclable) throws IOException {
            return new EventDeserializer.SaveableReaderBasedJsonParser(ctxt, _parserFeatures, null, _objectCodec,
                    _rootCharSymbols.makeChild(_factoryFeatures),
                    data, offset, offset + len, recyclable);
        }
    }
}
