package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;
import org.rakam.model.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
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
public class EventCollectorHttpService implements HttpService {
    final static Logger LOGGER = LoggerFactory.getLogger(EventCollectorHttpService.class);
    private final ObjectMapper jsonMapper = new ObjectMapper(new EventParserJsonFactory());

    private final Set<EventProcessor> processors;
    private final EventStore eventStore;
    private final Set<EventMapper> mappers;

    @Inject
    public EventCollectorHttpService(EventStore eventStore, EventDeserializer deserializer, Set<EventMapper> mappers, Set<EventProcessor> eventProcessors) {
        this.processors = eventProcessors;
        this.eventStore = eventStore;
        this.mappers = mappers;

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Event.class, deserializer);
        jsonMapper.registerModule(module);
    }

    private boolean processEvent(Event event) {

        for (EventProcessor processor : processors) {
            processor.process(event);
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

    @POST
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
            }
            boolean b = processEvent(event);
            request.response(b ? "1" : "0", b ? OK : BAD_GATEWAY).end();
        });
    }

    public static class EventParserJsonFactory extends JsonFactory {

        @Override
        protected JsonParser _createParser(char[] data, int offset, int len, IOContext ctxt,
                                           boolean recyclable) throws IOException {
            return new EventDeserializer.SaveableReaderBasedJsonParser(ctxt, _parserFeatures, null, _objectCodec,
                    _rootCharSymbols.makeChild(_factoryFeatures),
                    data, offset, offset+len, recyclable);
        }
    }
}
