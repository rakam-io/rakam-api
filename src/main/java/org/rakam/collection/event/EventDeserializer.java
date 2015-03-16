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
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.model.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.util.Tuple;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    private final FieldDependencyBuilder.FieldDependency moduleFields;

    @Inject
    public EventDeserializer(EventSchemaMetastore schemaRegistry, Set<EventMapper> eventMappers) {
        this.schemaRegistry = schemaRegistry;
        this.schemaCache = Maps.newConcurrentMap();

        FieldDependencyBuilder builder = new FieldDependencyBuilder();
        eventMappers.stream().forEach(mapper -> mapper.addFieldDependency(builder));

        this.moduleFields = builder.build();
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
        Schema avroSchema = schemaCache.get(key);
        List<SchemaField> schema = null;
        if (avroSchema == null) {
            schema = schemaRegistry.getSchema(project, collection);
            if (schema != null) {
                avroSchema = convertAvroSchema(schema);
                schemaCache.put(key, avroSchema);
            }
        }

        if (avroSchema == null) {
            ObjectNode node = jp.readValueAs(ObjectNode.class);
            List<SchemaField> fields = createSchemaFromArbitraryJson(node);
            moduleFields.constantFields.forEach(field -> addModuleField(fields, field));
            moduleFields.dependentFields.forEach((fieldName, field) -> addConditionalModuleField(fields, fieldName, field));

            schema = schemaRegistry.createOrGetSchema(project, collection, fields);
            avroSchema = convertAvroSchema(schema);
            schemaCache.put(key, avroSchema);

            GenericData.Record record = new GenericData.Record(avroSchema);
            fields.forEach(field -> record.put(field.getName(), getValue(node.get(field.getName()), field.getType())));
            return record;
        }

        GenericData.Record record = new GenericData.Record(avroSchema);
        List<SchemaField> newFields = null;

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            Schema.Field field = avroSchema.getField(fieldName);

            t = jp.nextToken();

            if (field == null) {
                FieldType type = getType(t, jp);
                if (type != null) {

                    if (newFields == null)
                        newFields = Lists.newArrayList();
                    newFields.add(new SchemaField(fieldName, type, true));

                    List<Schema.Field> avroFields = copyFields(avroSchema.getFields());
                    newFields.stream()
                            .filter(f -> !avroFields.stream().anyMatch(af -> af.name().equals(f.getName())))
                            .map(this::generateAvroSchema).forEach(x -> avroFields.add(x));

                    avroSchema = Schema.createRecord("collection", null, null, false);
                    avroSchema.setFields(avroFields);

                    field = avroFields.stream().filter(f -> f.name().equals(fieldName)).findAny().get();

                    record = new GenericData.Record(avroSchema);
                } else {
                    continue;
                }
            }

            putValue(t, jp, record, field);
        }

        if (newFields != null) {
            final List<SchemaField> finalNewFields = newFields;
            moduleFields.dependentFields.forEach((fieldName, field) -> addConditionalModuleField(finalNewFields, fieldName, field));

            List<SchemaField> newSchema = schemaRegistry.createOrGetSchema(project, collection, newFields);
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

    private void addConditionalModuleField(List<SchemaField> fields, String fieldName, List<SchemaField> newFields) {
        if (fields.stream().anyMatch(f -> f.getName().equals(fieldName))) {
            newFields.forEach(newField -> addModuleField(fields, newField));
        }
    }

    private void addModuleField(List<SchemaField> fields, SchemaField newField) {
        for (int i = 0; i < fields.size(); i++) {
            SchemaField field = fields.get(i);
            if(field.getName().equals(newField.getName())) {
                if(field.getType().equals(newField.getType())) {
                    return;
                }else {
                    fields.remove(i);
                    break;
                }
            }
        }
        fields.add(newField);
    }

    private Schema convertAvroSchema(List<SchemaField> fields) {
        List<Schema.Field> avroFields = fields.stream()
                .map(this::generateAvroSchema).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);
        schema.setFields(avroFields);
        return schema;
    }

    private Schema.Field generateAvroSchema(SchemaField field) {
            Schema es;
            if (field.isNullable()) {
                es = Schema.createUnion(Lists.newArrayList(Schema.create(NULL), getAvroSchema(field.getType())));
                return new Schema.Field(field.getName(), es, null, NullNode.getInstance());
            } else {
                es = getAvroSchema(field.getType());
                return new Schema.Field(field.getName(), es, null, null);
            }
    }

    private Schema getAvroSchema(FieldType type) {
        switch (type) {
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case ARRAY:
                return Schema.create(Schema.Type.ARRAY);
            case LONG:
                return Schema.create(Schema.Type.LONG);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN:
                return Schema.create(Schema.Type.DOUBLE);
            case DATE:
                return Schema.create(Schema.Type.INT);
            case HYPERLOGLOG:
                return Schema.create(Schema.Type.BYTES);
            case TIME:
                return Schema.create(Schema.Type.LONG);
            default:
                throw new IllegalStateException();
        }
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
                return jsonNode.asText();
            case DOUBLE:
                return jsonNode.doubleValue();
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
            case START_ARRAY:
                return FieldType.ARRAY;
            case VALUE_EMBEDDED_OBJECT:
                throw new JsonMappingException(format("nested properties is not supported: %s", jp.getValueAsString()));
            case VALUE_NUMBER_INT:
                return FieldType.LONG;
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
                if (type == FieldType.ARRAY) {
                    throw new IllegalArgumentException("nested properties is not supported");
                }
                return FieldType.ARRAY;
            default:
                throw new UnsupportedOperationException("unsupported json field type");
        }
    }

    public static List<SchemaField> createSchemaFromArbitraryJson(ObjectNode json) {
        Iterator<Map.Entry<String, JsonNode>> elements = json.fields();
        ArrayList<SchemaField> fields = new ArrayList<>();

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

