package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 21:48.
 */
@Path("/event")
public class EventCollectorService implements HttpService {
    final static Logger LOGGER = LoggerFactory.getLogger(EventCollectorService.class);
    private final ObjectMapper jsonMapper = new ObjectMapper(new EventParserJsonFactory());

    private final Set<EventProcessor> processors;
    private final Set<EventMapper> mappers;
    private final EventStore eventStore;

    @Inject
    public EventCollectorService(EventStore eventStore, EventSchemaMetastore schemas, Set<EventMapper> eventMappers, Set<EventProcessor> eventProcessors) {
        this.mappers = eventMappers;
        this.processors = eventProcessors;
        this.eventStore = eventStore;

        SimpleModule module = new SimpleModule();
        List<Schema.Field> moduleFields = mappers.stream().flatMap(mapper -> mapper.fields().stream()).collect(Collectors.toList());
        JsonDeserializer<Event> eventDeserializer = new EventDeserializer(schemas, moduleFields);
        module.addDeserializer(Event.class, eventDeserializer);
        jsonMapper.registerModule(module);

        // todo: test if existing collections has fields required by event mappers.
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
                request.response("json couldn't parsed").end();
                return;
            }
            request.response(processEvent(event) ? "1" : "0").end();
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
