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
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
    public Event deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
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
                    if(project == null || collection == null) {
                        // workaround for reading a skipped json node after processing whose json document.
                        ((SaveableReaderBasedJsonParser) jp).save();
                        jp.skipChildren();
                    } else {
                        properties = parseProperties(project, collection, jp);
                    }
                    break;
            }
        }
        if(project==null)
            throw new JsonMappingException("project is null");

        if(collection==null)
            throw new JsonMappingException("collection is null");

        if(properties==null) {
            SaveableReaderBasedJsonParser customJp = (SaveableReaderBasedJsonParser) jp;
            if(customJp.isSaved()) {
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
        if(schema == null) {
            schema = schemaRegistry.getSchema(project, collection);
            if(schema != null) {
                schemaCache.put(key, schema);
            }
        }

        if(schema == null) {
            ObjectNode json = jp.readValueAsTree();
            List<Schema.Field> fields = createSchemaFromArbitraryJson(json);
            fields.addAll(getFieldsUsedInModules());
            schema = schemaRegistry.createOrGetSchema(project, collection, fields);
            schemaCache.put(key, schema);
        }

        GenericData.Record record = new GenericData.Record(schema);
        Map<Schema.Field, Object> newFields = null;

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            Schema.Field field = schema.getField(fieldName);

            t = jp.nextToken();

            if(field == null) {
                Schema.Type avroType = getAvroType(t, jp);
                if(avroType!=null) {
                    Schema es = Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(avroType)));
                    Schema.Field avroField = new Schema.Field(fieldName, es, null, NullNode.getInstance());

                    if(newFields == null)
                        newFields = Maps.newHashMap();

                    newFields.put(avroField, jp.readValuesAs(Object.class));
                }
                continue;
            }

            putValue(t, jp, record, field);
        }

        if(newFields!=null) {
            Schema newSchema = schemaRegistry.createOrGetSchema(project, collection, newFields.keySet().stream().collect(toList()));
            schemaCache.put(key, newSchema);
            GenericData.Record newRecord = new GenericData.Record(newSchema);
            int oldSize = record.getSchema().getFields().size();
            for (Schema.Field field : newSchema.getFields()) {
                int pos = field.pos();
                if(pos >= oldSize)
                    newRecord.put(pos, newFields.get(field));
                else
                    newRecord.put(pos, record.get(pos));
            }
            record = newRecord;
        }

        return record;
    }

    private void putValue(JsonToken t, JsonParser jp, GenericData.Record record, Schema.Field field) throws IOException {
        Schema.Type type = field.schema().getTypes().get(1).getType();
        switch (t) {
            case VALUE_STRING:
                if(type == Schema.Type.STRING)
                    record.put(field.pos(), jp.getValueAsString());
                break;
            case VALUE_FALSE:
                if(type == Schema.Type.BOOLEAN)
                    record.put(field.pos(), false);
                else
                if(type == Schema.Type.STRING)
                    record.put(field.pos(), jp.getValueAsString());
                break;
            case VALUE_NUMBER_FLOAT:
                if(type == Schema.Type.FLOAT) {
                    record.put(field.pos(), jp.getFloatValue());
                }else
                if(type == Schema.Type.INT) {
                    record.put(field.pos(), jp.getIntValue());
                }else
                if(type == Schema.Type.LONG) {
                    record.put(field.pos(), jp.getLongValue());
                }else
                if(type == Schema.Type.STRING) {
                    record.put(field.pos(), jp.getValueAsString());
                }
                break;
            case VALUE_TRUE:
                if(type == Schema.Type.BOOLEAN)
                    record.put(field.pos(), true);
                else
                if(type == Schema.Type.STRING)
                    record.put(field.pos(), jp.getValueAsString());
                break;
            case VALUE_NULL:
                break;
            case VALUE_EMBEDDED_OBJECT:
                throw new JsonMappingException(format("nested properties is not supported: %s", field));
            case VALUE_NUMBER_INT:
                if(type == Schema.Type.FLOAT) {
                    record.put(field.pos(), jp.getFloatValue());
                }else
                if(type == Schema.Type.INT) {
                    record.put(field.pos(), jp.getIntValue());
                }else
                if(type == Schema.Type.LONG) {
                    record.put(field.pos(), jp.getLongValue());
                }else
                if(type == Schema.Type.STRING) {
                    record.put(field.pos(), jp.getValueAsString());
                }
                break;
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
            case VALUE_EMBEDDED_OBJECT:
                throw new JsonMappingException(format("nested properties is not supported: %s", jp.getValueAsString()));
            case VALUE_NUMBER_INT:
                return Schema.Type.LONG;
            default:
                return null;
        }
    }

    public List<Schema.Field> getFieldsUsedInModules() {
        return moduleFields.stream()
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()))
                .collect(Collectors.toList());
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

    public static List<Schema.Field> createSchemaFromArbitraryJson(ObjectNode json) {
        Iterator<Map.Entry<String, JsonNode>> elements = json.fields();
        ArrayList fields = new ArrayList();

        while(elements.hasNext()) {
            Map.Entry<String, JsonNode> next = elements.next();
            String key = next.getKey();
            JsonNodeType nodeType = next.getValue().getNodeType();
            Schema union = Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), Schema.create(getAvroType(nodeType))));
            fields.add(new Schema.Field(key, union, null, NullNode.getInstance()));
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
            return savedInputPtr>-1;
        }

        public void load() {
            _currToken = JsonToken.START_OBJECT;
            _inputPtr = savedInputPtr;
            _parsingContext = _parsingContext.createChildObjectContext(0, 0);
        }
    }
}

