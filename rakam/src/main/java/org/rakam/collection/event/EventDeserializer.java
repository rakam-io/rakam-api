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
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.ProjectNotExistsException;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.util.AvroUtil;
import org.rakam.util.ProjectCollection;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

public class EventDeserializer extends JsonDeserializer<Event> {
    private final Map<String, List<SchemaField>> sourceFields;
    private Logger logger = Logger.get(EventDeserializer.class);

    private final Metastore schemaRegistry;
    private final Map<ProjectCollection, Schema> schemaCache;

    @Inject
    public EventDeserializer(Metastore schemaRegistry, FieldDependencyBuilder.FieldDependency fieldDependency) {
        this.schemaRegistry = schemaRegistry;
        this.schemaCache = Maps.newConcurrentMap();
        this.sourceFields = fieldDependency.dependentFields;
    }

    @Override
    public Event deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
        return deserializeWithProject(jp, null);
    }

    public Event deserializeWithProject(JsonParser jp, String project) throws IOException {
        GenericData.Record properties = null;

        String collection = null;
        Event.EventContext context = null;

        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            jp.nextToken();

            switch (fieldName) {
                case "project":
                    if(project != null) {
                        throw new IllegalArgumentException("project is already set");
                    }
                    project = jp.getValueAsString();
                    break;
                case "collection":
                    collection = jp.getValueAsString().toLowerCase();
                    break;
                case "api":
                    context = jp.readValueAs(Event.EventContext.class);
                    break;
                case "properties":
                    if (project == null || collection == null) {
                        // workaround for reading a skipped json node after processing whose json document.
                        ((SaveableReaderBasedJsonParser) jp).save();
                        jp.skipChildren();
                    } else {
                        try {
                            properties = parseProperties(project, collection, jp);
                        } catch (ProjectNotExistsException e) {
                            throw Throwables.propagate(e);
                        }
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
                try {
                    properties = parseProperties(project, collection, jp);
                } catch (ProjectNotExistsException e) {
                    throw Throwables.propagate(e);
                }
            } else {
                throw new JsonMappingException("properties is null");
            }
        }
        return new Event(project, collection, properties, context);
    }

    public Schema convertAvroSchema(List<SchemaField> fields) {
        List<Schema.Field> avroFields = fields.stream()
                .map(AvroUtil::generateAvroSchema).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);

        sourceFields.keySet().stream()
                .filter(s -> !avroFields.stream().anyMatch(af -> af.name().equals(s)))
                .map(n -> new Schema.Field(n, Schema.create(Schema.Type.NULL), "", null))
                .forEach(x -> avroFields.add(x));

        schema.setFields(avroFields);

        return schema;
    }

    private GenericData.Record parseProperties(String project, String collection, JsonParser jp) throws IOException, ProjectNotExistsException {
        ProjectCollection key = new ProjectCollection(project, collection);
        Schema avroSchema = schemaCache.get(key);
        List<SchemaField> schema;
        if (avroSchema == null) {
            schema = schemaRegistry.getCollection(project, collection);
            if (schema != null) {
                avroSchema = convertAvroSchema(schema);

                schemaCache.put(key, avroSchema);
            }
        }

        if (avroSchema == null) {
            ObjectNode node = jp.readValueAs(ObjectNode.class);
            Set<SchemaField> fields = createSchemaFromArbitraryJson(node);

            schema = schemaRegistry.getOrCreateCollectionFieldList(project, collection, fields);
            avroSchema = convertAvroSchema(schema);

            schemaCache.put(key, avroSchema);

            GenericData.Record record = new GenericData.Record(avroSchema);
            fields.forEach(field -> record.put(field.getName(), getValue(node.get(field.getName()), field.getType())));
            return record;
        }

        GenericData.Record record = new GenericData.Record(avroSchema);
        Set<SchemaField> newFields = null;

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            Schema.Field field = avroSchema.getField(fieldName);

            t = jp.nextToken();

            if (field == null) {
                FieldType type = getType(t, jp);
                if (type != null) {
                    if (newFields == null)
                        newFields = new HashSet<>();
                    SchemaField newField = new SchemaField(fieldName, type, true);

                    newFields.add(newField);
                    avroSchema = createNewSchema(avroSchema, newField);
                    field = avroSchema.getFields().stream().filter(f -> f.name().equals(fieldName)).findAny().get();

                    GenericData.Record newRecord = new GenericData.Record(avroSchema);
                    for (Schema.Field f : record.getSchema().getFields()) {
                        newRecord.put(f.name(), record.get(f.name()));
                    }
                    record = newRecord;
                } else {
                    continue;
                }
            } else {
                if(field.schema().getType() == Schema.Type.NULL) {
                    // TODO: get rid of this loop.
                    for (SchemaField schemaField : sourceFields.get(fieldName)) {
                        if(avroSchema.getField(schemaField.getName()) == null) {
                            if(newFields == null) {
                                newFields = new HashSet<>();
                            }
                            newFields.add(schemaField);
                        }
                    }
                }
            }

            putValue(t, jp, record, field, field.pos());
        }

        if (newFields != null) {
            List<SchemaField> newSchema = schemaRegistry.getOrCreateCollectionFieldList(project, collection, newFields);
            Schema newAvroSchema = convertAvroSchema(newSchema);

            schemaCache.put(key, newAvroSchema);
            GenericData.Record newRecord = new GenericData.Record(newAvroSchema);
            for (Schema.Field field : record.getSchema().getFields()) {
                newRecord.put(field.name(), record.get(field.name()));
            }
            record = newRecord;
        }

        return record;
    }

    private Schema createNewSchema(Schema currentSchema, SchemaField newField) {
        ArrayList<SchemaField> newFields = new ArrayList<>();
        newFields.add(newField);

        List<Schema.Field> avroFields = copyFields(currentSchema.getFields());

        newFields.stream()
                .filter(f -> !avroFields.stream().anyMatch(af -> af.name().equals(f.getName())))
                .map(AvroUtil::generateAvroSchema).forEach(x -> avroFields.add(x));

        sourceFields.keySet().stream()
                .filter(s -> !avroFields.stream().anyMatch(af -> af.name().equals(s)))
                .map(n -> new Schema.Field(n, Schema.create(Schema.Type.NULL), "", null))
                .forEach(x -> avroFields.add(x));

        Schema avroSchema = Schema.createRecord("collection", null, null, false);
        avroSchema.setFields(avroFields);

        return avroSchema;
    }

    private Object getValue(JsonNode jsonNode, FieldType type) {

        if (jsonNode == null)
            return null;

        switch (type) {
            case BOOLEAN:
                return jsonNode.asBoolean();
            case LONG:
                return jsonNode.longValue();
            case STRING:
            case TIME:
            case TIMESTAMP:
                return jsonNode.asText();
            case DOUBLE:
                return jsonNode.doubleValue();
            case ARRAY_STRING:
            case ARRAY_TIME:
            case ARRAY_TIMESTAMP:
                return stream(jsonNode.spliterator(), false).map(x -> x.asText()).collect(toList());
            case ARRAY_LONG:
                return stream(jsonNode.spliterator(), false).map(x -> x.asLong()).collect(toList());
            case ARRAY_DOUBLE:
                return stream(jsonNode.spliterator(), false).map(x -> x.asDouble()).collect(toList());
            case ARRAY_BOOLEAN:
                return stream(jsonNode.spliterator(), false).map(x -> x.asBoolean()).collect(toList());
            default:
                throw new IllegalStateException();
        }
    }

    private Schema.Type getActualType(Schema.Field field) {
        Schema schema = field.schema();
        Schema.Type schemaType = schema.getType();

        if (schemaType == Schema.Type.UNION) {
            schema = schema.getTypes().get(1);
            return schema.getType();
        } else {
            return schemaType;
        }
    }

    private void putValue(JsonToken t, JsonParser jp, GenericData.Record record, Schema.Field field, int pos) throws IOException {
        switch (getActualType(field)) {
            case NULL:
                switch (t) {
                    case VALUE_TRUE:
                        record.put(pos, true);
                        break;
                    case VALUE_FALSE:
                        record.put(pos, false);
                        break;
                    case VALUE_NUMBER_FLOAT:
                        record.put(pos, jp.getValueAsDouble());
                        break;
                    case VALUE_NUMBER_INT:
                        record.put(pos, jp.getValueAsLong());
                        break;
                    case VALUE_STRING:
                        record.put(pos, jp.getValueAsString());
                        break;
                    case VALUE_NULL:
                        record.put(pos, null);
                        break;
                    default:
                        throw new IllegalArgumentException("unknown value for magic field "+field.name());
                }
                break;
            case STRING:
                // TODO: is it a good idea to cast the value automatically?
//                if (t == JsonToken.VALUE_STRING)
                    record.put(pos, jp.getValueAsString());
                break;
            case BOOLEAN:
                if (t == JsonToken.VALUE_STRING)
                    record.put(pos, jp.getValueAsBoolean());
                break;
            case LONG:
                if (t == JsonToken.VALUE_NUMBER_INT)
                    record.put(pos, jp.getValueAsLong());
                break;
            case INT:
                if (t == JsonToken.VALUE_NUMBER_INT)
                    record.put(pos, jp.getValueAsInt());
                break;
            case DOUBLE:
                if (t == JsonToken.VALUE_NUMBER_FLOAT || t == JsonToken.VALUE_NUMBER_INT)
                    record.put(pos, jp.getValueAsDouble());
                break;
            case FLOAT:
                if (t == JsonToken.VALUE_NUMBER_FLOAT) {
                    Number numberValue = jp.getNumberValue();
                    if (numberValue != null)
                        record.put(pos, numberValue.floatValue());
                }
                break;
            case UNION:
                break;
            case ARRAY:
                throw new UnsupportedOperationException("array type is not supported yet");
//                if(jp.isExpectedStartArrayToken()) {
//                    ArrayList<Object> objects = new ArrayList<>();
//                    for (t = jp.nextToken(); t != JsonToken.END_ARRAY; t = jp.nextToken()) {
//                        objects.add(jp.getValueAsString());
//                    }
//                    record.put(pos, new GenericData.Array(schema, objects));
//                }
//                break;
            default:
                throw new JsonMappingException(format("nested properties is not supported."));
        }
    }

    private FieldType getType(JsonToken t, JsonParser jp) throws IOException {
        switch (t) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return FieldType.STRING;
            case VALUE_FALSE:
                return FieldType.BOOLEAN;
            case VALUE_NUMBER_FLOAT:
                return FieldType.DOUBLE;
            case VALUE_TRUE:
                return FieldType.BOOLEAN;
            case VALUE_NUMBER_INT:
                return FieldType.LONG;
            case START_ARRAY:
                t = jp.nextToken();
                try {
                    return getType(t, jp).convertToArrayType();
                } catch (IOException e) {
                    // fallback to JsonMappingException for nested properties.
                }
            case VALUE_EMBEDDED_OBJECT:
                throw new JsonMappingException(format("nested properties is not supported: %s", jp.getValueAsString()));
            default:
                return null;
        }
    }

    public static List<Schema.Field> copyFields(List<Schema.Field> fields) {
        return fields.stream()
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()))
                .collect(toList());
    }

    public static FieldType getTypeFromJsonNode(JsonNode jsonNode) {
        JsonNodeType nodeType = jsonNode.getNodeType();
        switch (nodeType) {
            case NUMBER:
                switch (jsonNode.numberType()) {
                    case INT:
                    case LONG:
                    case BIG_INTEGER:
                        return FieldType.LONG;
                    default:
                        return FieldType.DOUBLE;
                }
            case STRING:
                return FieldType.STRING;
            case BOOLEAN:
                return FieldType.BOOLEAN;
            case NULL:
                return null;
            case ARRAY:
                FieldType type = getTypeFromJsonNode(jsonNode.get(0));
                if (!type.isArray()) {
                    throw new IllegalArgumentException("nested properties is not supported");
                }
                return type;
            default:
                throw new UnsupportedOperationException("unsupported json field type");
        }
    }

    public static Set<SchemaField> createSchemaFromArbitraryJson(ObjectNode json) {
        Iterator<Map.Entry<String, JsonNode>> elements = json.fields();
        Set<SchemaField> fields = new HashSet<>();

        while (elements.hasNext()) {
            Map.Entry<String, JsonNode> next = elements.next();
            String key = next.getKey();
            FieldType fieldType = getTypeFromJsonNode(next.getValue());
            if (fieldType != null) {
                fields.add(new SchemaField(key, fieldType, true));
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

