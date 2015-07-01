package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;
import org.rakam.collection.Event;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SystemEventListener;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.Set;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 21:48.
 */
@Path("/event")
@Api(value = "/event", description = "Event collection module", tags = "event")
public class EventHttpService extends HttpService {
    final static Logger LOGGER = LoggerFactory.getLogger(EventHttpService.class);
    private final ObjectMapper jsonMapper = new ObjectMapper(new EventParserJsonFactory());
    private final Metastore metastore;

    private final Set<EventProcessor> processors;
    private final EventStore eventStore;
    private final Set<EventMapper> mappers;
    private final Set<SystemEventListener> systemEventListeners;

    @Inject
    public EventHttpService(EventStore eventStore, Metastore metastore, EventDeserializer deserializer, Set<EventMapper> mappers, Set<EventProcessor> eventProcessors, Set<SystemEventListener> systemEventListeners) {
        this.processors = eventProcessors;
        this.eventStore = eventStore;
        this.mappers = mappers;
        this.metastore = metastore;
        this.systemEventListeners = systemEventListeners;

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Event.class, deserializer);
        jsonMapper.registerModule(module);
    }

    private boolean processEvent(Event event) {
        for (EventProcessor processor : processors) {
            try {
                processor.process(event);
            } catch (Exception e) {
                LOGGER.error("An error occurred while processing event in "+processor.getClass().getName(), e);
                return false;
            }
        }

        for (EventMapper mapper : mappers) {
            try {
                mapper.map(event);
            } catch (Exception e) {
                LOGGER.error("An error occurred while processing event in "+mapper.getClass().getName(), e);
                return false;
            }
        }

        for (EventMapper mapper : mappers) {
            mapper.map(event);
        }

        try {
            eventStore.store(event);
        } catch (Exception e) {
            LOGGER.error("error while storing event.", e);
            return false;
        }

        return true;
    }

    /**
     * @api {post} /event/collect Collect event
     * @apiVersion 0.1.0
     * @apiName CollectEvent
     * @apiGroup event
     * @apiDescription Stores event data for specified project and collection tuple.
     * @apiParam {String} project   Project tracker code that the event belongs.
     * @apiParam {String} collection    Collection name. (pageView, register etc.)
     * @apiParam {Object} properties    The properties of the event.
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/event/collect' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "collection": "pageView", "properties": { "url": "http://rakam.io" } }'
     */
    @POST
    @ApiOperation(value = "Collect event",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/collect")
    public void collect(RakamHttpRequest request) {
        request.bodyHandler(buff -> {
            Event event;
            try {
                event = jsonMapper.readValue(buff, Event.class);
            } catch (JsonMappingException e) {
                request.response(e.getMessage()).end();
                return;
            } catch (IOException e) {
                request.response("json couldn't parsed", BAD_REQUEST).end();
                return;
            } catch (Exception e) {
                request.response(e.getMessage(), BAD_REQUEST).end();
                return;
            }
            boolean eventProcessed = processEvent(event);
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
