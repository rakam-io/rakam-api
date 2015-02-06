package org.rakam.collection.event;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.server.RouteMatcher;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.Path;
import org.rakam.util.JsonHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 21:48.
 */
@Path("/event")
public class EventCollectorService implements HttpService {
    public final ExecutorService executor = new ThreadPoolExecutor(35, 50, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());
    private final Producer<byte[], byte[]> kafkaProducer;
    private final Set<EventProcessor> processors;
    private final Set<EventMapper> mappers;
    private final AvroSchemaRegistryService schemas;
    private final Map<String, AvroSchema> lastSchema;
    private static ObjectMapper mapper = new ObjectMapper(new AvroFactory());

    @Inject
    public EventCollectorService(Producer producer, AvroSchemaRegistryService schemas, Set<EventMapper> eventMappers, Set<EventProcessor> eventProcessors) {
        this.mappers = eventMappers;
        this.processors = eventProcessors;
        this.kafkaProducer = producer;
        this.schemas = schemas;
        this.lastSchema = new ConcurrentHashMap<>();
    }

    private boolean processEvent(ObjectNode event) {
        // TODO: make it thread-local so that we can re-use same buffer.
        ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer(64);
        ChannelBufferOutputStream out = new ChannelBufferOutputStream(channelBuffer);

        String project = event.get("project").asText();
        String collection = event.get("collection").asText();
        String key = project+"_"+ collection;
        AvroSchema projectSchema = lastSchema.get(key);

        try {
            if(projectSchema==null) {
                Schema newSchema = schemas.getSchema(key);
                if(newSchema==null) {
                    List<Schema.Field> schemaFromArbitraryJson = createSchemaFromArbitraryJson(event);
                    newSchema = schemas.addFieldsAndGetSchema(key, schemaFromArbitraryJson);
                }
                AvroSchema value = new AvroSchema(newSchema);
                lastSchema.put(key, value);
                projectSchema = value;
                mapper.writer(value).writeValue(out, event);
            }else {
                mapper.writer(projectSchema).writeValue(out, event);
            }
        }  catch (JsonMappingException e) {
            Iterator<Map.Entry<String, JsonNode>> elements = event.fields();
            List<Schema.Field> fields = projectSchema.getAvroSchema().getFields();
            ArrayList<Schema.Field> newFields = null;
            while(elements.hasNext()) {
                Map.Entry<String, JsonNode> next = elements.next();
                Optional<Schema.Field> any = fields.stream().filter(x -> x.name().equals(next.getKey())).findAny();
                if(any.isPresent()) {
                    Schema schema = any.get().schema();
                    if(getJsonType(schema) != next.getValue().getNodeType())
                        event.remove(next.getKey());
                } else {
                    if(newFields == null)
                        newFields = Lists.newArrayList();

                    JsonNodeType nodeType = next.getValue().getNodeType();
                    Schema union = Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(getAvroType(nodeType))));
                    fields.add(new Schema.Field(key, union, null, NullNode.getInstance()));
                }
            }
            if(newFields != null) {
                schemas.addFieldsAndGetSchema(key, newFields);
            }
        } catch (IOException e) {
            return false;
        }

        kafkaProducer.send(new KeyedMessage<>(project, collection.getBytes(), channelBuffer.array()));
        return true;
    }

    @Override
    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {
        routeMatcher
            .add("/collect", HttpMethod.POST, request ->
                    request.bodyHandler(buff -> {
                        ObjectNode event;
                        try {
                            event = JsonHelper.read(buff);
                        } catch (IOException e) {
                            request.response("0").end();
                            return;
                        }
                        executor.execute(() -> request.response(processEvent(event) ? "1" : "0").end());
                    }));
    }

    public static List<Schema.Field> createSchemaFromArbitraryJson(ObjectNode json) {
        Iterator<Map.Entry<String, JsonNode>> elements = json.fields();
        ArrayList fields = new ArrayList();
        fields.add(new Schema.Field("project", Schema.create(Schema.Type.STRING), null, null));

        while(elements.hasNext()) {
            Map.Entry<String, JsonNode> next = elements.next();
            String key = next.getKey();
            if(key.equals("project"))
                continue;
            JsonNodeType nodeType = next.getValue().getNodeType();
            Schema union = Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(getAvroType(nodeType))));
            fields.add(new Schema.Field(key, union, null, NullNode.getInstance()));
        }

        return fields;
    }

    public static Schema.Type getAvroType(JsonNodeType jsonType) {
        switch (jsonType) {
            case NUMBER:
                return Schema.Type.FLOAT;
            case STRING:
                return Schema.Type.STRING;
            case BOOLEAN:
                return Schema.Type.BOOLEAN;
            case NULL:
                return Schema.Type.NULL;
            case OBJECT:
                return Schema.Type.RECORD;
            case ARRAY:
                return Schema.Type.ARRAY;
            default:
                throw new UnsupportedOperationException("unsupported json field type");
        }
    }

    public static JsonNodeType getJsonType(Schema avroSchema) {
        switch (avroSchema.getType()) {
            case FLOAT:
            case DOUBLE:
            case INT:
            case LONG:
                return JsonNodeType.NUMBER;
            case STRING:
                return JsonNodeType.STRING;
            case BOOLEAN:
                return JsonNodeType.BOOLEAN;
            case NULL:
                return JsonNodeType.NULL;
            case ARRAY:
                return JsonNodeType.ARRAY;
            case UNION:
                List<Schema> types = avroSchema.getTypes();
                if(types.size() == 1)
                    return getJsonType(types.get(0));
                if(types.size() == 2) {
                    return getJsonType(types.get(types.get(0).equals(Schema.Type.NULL) ? 1 : 0));
                }
                throw new UnsupportedOperationException("unsupported avro field type");
            default:
                throw new UnsupportedOperationException("unsupported avro field type");
        }
    }
}
