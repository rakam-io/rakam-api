package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;
import com.fasterxml.jackson.core.sym.CharsToNameCanonicalizer;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.node.NullNode;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.model.Event;
import org.rakam.util.Tuple;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.avro.Schema.Type.NULL;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 23:50.
 */
public class EventDeserializer extends JsonDeserializer<Event> {
    private final EventSchemaMetastore schemaRegistry;
    private final Map<Tuple<String, String>, Schema> schemaCache;
    private final List<Schema.Field> moduleFields;

    @Inject
    public EventDeserializer(EventSchemaMetastore schemaRegistry, List<Schema.Field> moduleFields) {
        this.schemaRegistry = schemaRegistry;
        this.schemaCache = Maps.newConcurrentMap();
        this.moduleFields = moduleFields;
    }

    @Override
    public Event deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
        String project = null, collection = null;
        GenericData.Record properties = null;

        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            t = jp.nextToken();
            switch (fieldName) {
                case "project":
                    project = jp.getValueAsString();
                    break;
                case "collection":
                    collection = jp.getValueAsString();
                    break;
                case "properties":
                    if (project == null || collection == null) {
                        // workaround for reading a skipped json node after processing whose json document.
                        ((SaveableReaderBasedJsonParser) jp).save();
                        jp.skipChildren();
                    } else {
                        properties = parseProperties(project, collection, jp);
                    }
                    break;
            }
        }
        if (project == null)
            throw new JsonMappingException("project is null");

        if (collection == null)
            throw new JsonMappingException("collection is null");

        if (properties == null) {
            SaveableReaderBasedJsonParser customJp = (SaveableReaderBasedJsonParser) jp;
            if (customJp.isSaved()) {
                customJp.load();
                properties = parseProperties(project, collection, jp);
            } else {
                throw new JsonMappingException("properties is null");
            }
        }
        return Event.create(project, collection, properties);
    }

    private GenericData.Record parseProperties(String project, String collection, JsonParser jp) throws IOException {
        Tuple key = new Tuple(project, collection);
        Schema schema = schemaCache.get(key);
        if (schema == null) {
            schema = schemaRegistry.getSchema(project, collection);
            if (schema != null) {
                schemaCache.put(key, schema);
            }
        }

        if (schema == null) {
            ObjectNode node = jp.readValueAs(ObjectNode.class);
            List<Schema.Field> fields = createSchemaFromArbitraryJson(node);
            for (Schema.Field field : copyFields(moduleFields)) {
                Optional<Schema.Field> first = fields.stream().filter(x -> x.name().equals(field.name())).findFirst();
                if (first.isPresent()) {
                    Schema.Field existingField = first.get();
                    if (!existingField.schema().equals(field.schema())) {
                        fields.set(fields.indexOf(existingField), field);
                    }
                } else {
                    fields.add(field);
                }
            }
            schema = schemaRegistry.createOrGetSchema(project, collection, fields);
            schemaCache.put(key, schema);

            GenericData.Record record = new GenericData.Record(schema);
            fields.forEach(field -> record.put(field.name(), getValue(node.get(field.name()), field.schema())));
            return record;
        }

        GenericData.Record record = new GenericData.Record(schema);
        List<Schema.Field> newFields = null;

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            Schema.Field field = schema.getField(fieldName);

            t = jp.nextToken();

            if (field == null) {
                Schema.Type avroType = getAvroType(t, jp);
                if (avroType != null) {
                    Schema es = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), Schema.create(avroType)));
                    field = new Schema.Field(fieldName, es, null, NullNode.getInstance());

                    if (newFields == null)
                        newFields = Lists.newArrayList();
                    newFields.add(field);

                    List<Schema.Field> fields = copyFields(schema.getFields());
                    fields.add(field);
                    schema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
                    schema.setFields(fields);
                    record = new GenericData.Record(schema);
                } else {
                    continue;
                }
            }

            putValue(t, jp, record, field);
        }

        if (newFields != null) {
            Schema newSchema = schemaRegistry.createOrGetSchema(project, collection, copyFields(newFields));
            schemaCache.put(key, newSchema);
            GenericData.Record newRecord = new GenericData.Record(newSchema);
            for (Schema.Field field : record.getSchema().getFields()) {
                newRecord.put(field.name(), record.get(field.name()));
            }
            record = newRecord;
        }

        return record;
    }

    private Object getValue(JsonNode jsonNode, Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            schema = schema.getTypes().get(1);
        }

        if (jsonNode == null)
            return null;

        switch (schema.getType()) {
            case BOOLEAN:
                return jsonNode.asBoolean();
            case LONG:
                return jsonNode.longValue();
            case INT:
                return jsonNode.intValue();
            case STRING:
                return jsonNode.asText();
            case DOUBLE:
                return jsonNode.doubleValue();
            case FLOAT:
                return jsonNode.floatValue();
            case NULL:
                return null;
            case ARRAY:
                return stream(jsonNode.spliterator(), false).map(x -> x.asText()).collect(toList());
            default:
                throw new IllegalStateException();
        }
    }

    private void putValue(JsonToken t, JsonParser jp, GenericData.Record record, Schema.Field field) throws IOException {
        Schema schema = field.schema();
        Schema.Type schemaType = schema.getType();

        Schema.Type type;
        if (schemaType == Schema.Type.UNION) {
            schema = schema.getTypes().get(1);
            type = schema.getType();
        } else {
            type = schemaType;
        }
        switch (type) {
            case STRING:
                if (t == JsonToken.VALUE_STRING)
                    record.put(field.pos(), jp.getValueAsString());
                break;
            case BOOLEAN:
                if (t == JsonToken.VALUE_STRING)
                    record.put(field.pos(), jp.getValueAsBoolean());
                break;
            case LONG:
                if (t == JsonToken.VALUE_NUMBER_INT)
                    record.put(field.pos(), jp.getValueAsLong());
                break;
            case INT:
                if (t == JsonToken.VALUE_NUMBER_INT)
                    record.put(field.pos(), jp.getValueAsInt());
                break;
            case DOUBLE:
                if (t == JsonToken.VALUE_NUMBER_FLOAT)
                    record.put(field.pos(), jp.getValueAsDouble());
                break;
            case FLOAT:
                if (t == JsonToken.VALUE_NUMBER_FLOAT) {
                    Number numberValue = jp.getNumberValue();
                    if (numberValue != null)
                        record.put(field.pos(), numberValue.floatValue());
                }
                break;
            case UNION:
                break;
            case ARRAY:
                if(jp.isExpectedStartArrayToken()) {
                    ArrayList<Object> objects = new ArrayList<>();
                    for (t = jp.nextToken(); t != JsonToken.END_ARRAY; t = jp.nextToken()) {
                        objects.add(jp.getValueAsString());
                    }
                    record.put(field.pos(), new GenericData.Array(schema, objects));
                }
                break;
            default:
                throw new JsonMappingException(format("nested properties is not supported: %s", field));
        }
    }

    private Schema.Type getAvroType(JsonToken t, JsonParser jp) throws IOException {
        switch (t) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return Schema.Type.STRING;
            case VALUE_FALSE:
                return Schema.Type.BOOLEAN;
            case VALUE_NUMBER_FLOAT:
                return Schema.Type.FLOAT;
            case VALUE_TRUE:
                return Schema.Type.BOOLEAN;
            case START_ARRAY:
                return Schema.Type.ARRAY;
            case VALUE_EMBEDDED_OBJECT:
                throw new JsonMappingException(format("nested properties is not supported: %s", jp.getValueAsString()));
            case VALUE_NUMBER_INT:
                return Schema.Type.LONG;
            default:
                return null;
        }
    }

    public static List<Schema.Field> copyFields(List<Schema.Field> fields) {
        return fields.stream()
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()))
                .collect(toList());
    }

    public static Schema getAvroType(JsonNode jsonNode) {
        JsonNodeType nodeType = jsonNode.getNodeType();
        switch (nodeType) {
            case NUMBER:
                switch (jsonNode.numberType()) {
                    case INT:
                    case LONG:
                    case BIG_INTEGER:
                        return Schema.create(Schema.Type.LONG);
                    case BIG_DECIMAL:
                    case FLOAT:
                    case DOUBLE:
                        return Schema.create(Schema.Type.FLOAT);
                    default:
                        return Schema.create(Schema.Type.FLOAT);
                }
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case NULL:
                return null;
            case ARRAY:
                Schema avroType = getAvroType(jsonNode.get(0));
                Schema.Type type = avroType.getType();
                if (type == Schema.Type.ARRAY) {
                    throw new IllegalArgumentException("nested properties is not supported");
                }
                Schema union = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), avroType));
                return Schema.createArray(union);
            default:
                throw new UnsupportedOperationException("unsupported json field type");
        }
    }

    public static List<Schema.Field> createSchemaFromArbitraryJson(ObjectNode json) {
        Iterator<Map.Entry<String, JsonNode>> elements = json.fields();
        ArrayList fields = new ArrayList();

        while (elements.hasNext()) {
            Map.Entry<String, JsonNode> next = elements.next();
            String key = next.getKey();
            Schema avroSchema = getAvroType(next.getValue());
            if (avroSchema != null) {
                Schema union = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), avroSchema));
                fields.add(new Schema.Field(key, union, null, NullNode.getInstance()));
            }
        }

        return fields;


    }

    public static class SaveableReaderBasedJsonParser extends ReaderBasedJsonParser {
        private int savedInputPtr = -1;

        public SaveableReaderBasedJsonParser(IOContext ctxt, int features, Reader r, ObjectCodec codec, CharsToNameCanonicalizer st, char[] inputBuffer, int start, int end, boolean bufferRecyclable) {
            super(ctxt, features, r, codec, st, inputBuffer, start, end, bufferRecyclable);
        }

        public void save() {
            savedInputPtr = _inputPtr;
        }

        public boolean isSaved() {
            return savedInputPtr > -1;
        }

        public void load() {
            _currToken = JsonToken.START_OBJECT;
            _inputPtr = savedInputPtr;
            _parsingContext = _parsingContext.createChildObjectContext(0, 0);
        }
    }
}

