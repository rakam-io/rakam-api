package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.report.Event;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.json.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.rakam.server.http.HttpServer.errorMessage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 21:48.
 */
@Path("/event")
public class EventHttpService extends HttpService {
    final static Logger LOGGER = LoggerFactory.getLogger(EventHttpService.class);
    private final ObjectMapper jsonMapper = new ObjectMapper(new EventParserJsonFactory());
    private final EventSchemaMetastore metastore;

    private final Set<EventProcessor> processors;
    private final EventStore eventStore;
    private final Set<EventMapper> mappers;

    @Inject
    public EventHttpService(EventStore eventStore, EventSchemaMetastore metastore, EventDeserializer deserializer, Set<EventMapper> mappers, Set<EventProcessor> eventProcessors) {
        this.processors = eventProcessors;
        this.eventStore = eventStore;
        this.mappers = mappers;
        this.metastore = metastore;

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

    /**
     * @api {post} /event/collect Collect event
     * @apiVersion 0.1.0
     * @apiName CollectEvent
     * @apiGroup event
     * @apiDescription Stores event data for specified project and collection tuple.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Bad Gateway
     *     0
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     1
     *
     * @apiParam {String} project   Project tracker code that the event belongs.
     * @apiParam {String} collection    Collection name. (pageView, register etc.)
     * @apiParam {Object} properties    The properties of the event.
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/event/collect' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{ "project": "projectId", "collection": "pageView", "properties": { "url": "http://rakam.io" } }'
     */
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

    /**
     * @api {post} /event/schema Get event schema
     * @apiVersion 0.1.0
     * @apiName GetEventSchema
     * @apiGroup event
     * @apiDescription Returns event metadata.
     *
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId"}
     *
     * @apiSuccess (200) {Object[]} collections  List of collections
     * @apiSuccess (200) {String} collections.name  The name of the collection
     * @apiSuccess (200) {Object[]} collections.fields  The name of the collection
     * @apiSuccess (200) {String} collections.fields.name  The name of the collection
     * @apiSuccess (200) {String="STRING","ARRAY","LONG","DOUBLE","BOOLEAN","DATE","HYPERLOGLOG","TIME"} collections.fields.type The data type of the field
     * @apiSuccess (200) {Boolean} collections.fields.nullable The value can be null
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *   {"collections":[{"name":"pageView","fields":[{"name":"url","type":"STRING","nullable":true},{"name":"id","type":"LONG","nullable":false}]}]}
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/event/schema' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId"}'
     */
    @JsonRequest
    @Path("/schema")
    public Object schema(JsonNode json) {
        JsonNode project = json.get("project");

        if (project == null || !project.isTextual()) {
            return errorMessage("project parameter is required", 400);
        }

        return new JsonResponse() {
            public final List collections = metastore.getSchemas(project.asText()).entrySet().stream()
                    .map(entry -> new JsonResponse() {
                        public final String name = entry.getKey();
                        public final List<SchemaField> fields = entry.getValue();
            }).collect(Collectors.toList());
        };
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
