package org.rakam.collection.event;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.NotExistsException;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.util.AvroUtil;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.avro.Schema.Type.NULL;

public class EventDeserializer extends JsonDeserializer<Event> {
    private final Map<String, List<SchemaField>> conditionalMagicFields;

    private static final Pattern DATE_PATTERN = Pattern.compile("^\\d{4}\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[01])$");
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^([\\+-]?\\d{4}(?!\\d{2}\\b))((-?)((0[1-9]|1[0-2])(\\3([12]\\d|0[1-9]|3[01]))?|W([0-4]\\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\\d|[12]\\d{2}|3([0-5]\\d|6[1-6])))([T\\s]((([01]\\d|2[0-3])((:?)[0-5]\\d)?|24\\:?00)([\\.,]\\d+(?!:))?)?(\\17[0-5]\\d([\\.,]\\d+)?)?([zZ]|([\\+-])([01]\\d|2[0-3]):?([0-5]\\d)?)?)?)?$");
    private static final Pattern TIME_PATTERN = Pattern.compile("^([2][0-3]|[0-1][0-9]|[1-9]):[0-5][0-9]:([0-5][0-9]|[6][0])$");

    private final Metastore metastore;
    private final Cache<ProjectCollection, Map.Entry<List<SchemaField>, Schema>> schemaCache  = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS).build();

    @Inject
    public EventDeserializer(Metastore metastore, FieldDependencyBuilder.FieldDependency fieldDependency) {
        this.metastore = metastore;
        this.conditionalMagicFields = fieldDependency.dependentFields;
    }

    @Override
    public Event deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
        return deserializeWithProject(jp, null);
    }

    public Event deserializeWithProject(JsonParser jp, String project) throws IOException, RakamException {
        Map.Entry<List<SchemaField>, GenericData.Record> properties = null;

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
                        if(jp.getValueAsString() != null) {
                            throw new RakamException("project is already set", BAD_REQUEST);
                        }
                    } else {
                        project = jp.getValueAsString();
                    }
                    break;
                case "collection":
                    collection = jp.getValueAsString().toLowerCase();
                    break;
                case "api":
                    context = jp.readValueAs(Event.EventContext.class);
                    break;
                case "properties":
                    if (project == null || collection == null) {
                        throw new JsonMappingException("'project' and 'collection' fields must be located before 'properties' field.");
                    } else {
                        try {
                            properties = parseProperties(project, collection, jp);
                        } catch (NotExistsException e) {
                            throw Throwables.propagate(e);
                        }
                        t = jp.getCurrentToken();

                        if(t != JsonToken.END_OBJECT) {
                            if(t == JsonToken.START_OBJECT) {
                                throw new RakamException("Nested properties are not supported", BAD_REQUEST);
                            } else {
                                throw new RakamException("Error while deserializing event", INTERNAL_SERVER_ERROR);
                            }
                        }
                    }
                    break;
                default:
                    throw new RakamException(String.format("Unrecognized field '%s' ", fieldName), BAD_REQUEST);
            }
        }
        if (properties == null) {
            throw new JsonMappingException("properties is null");
        }
        return new Event(project, collection, context, properties.getKey(), properties.getValue());
    }

    public Schema convertAvroSchema(List<SchemaField> fields) {
        List<Schema.Field> avroFields = fields.stream()
                .map(AvroUtil::generateAvroSchema).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);

        conditionalMagicFields.keySet().stream()
                .filter(s -> !avroFields.stream().anyMatch(af -> af.name().equals(s)))
                .map(n -> new Schema.Field(n, Schema.create(NULL), "", null))
                .forEach(x -> avroFields.add(x));

        schema.setFields(avroFields);
        return schema;
    }

    private Map.Entry<List<SchemaField>, GenericData.Record> parseProperties(String project, String collection, JsonParser jp) throws IOException, NotExistsException {
        ProjectCollection key = new ProjectCollection(project, collection);
        Map.Entry<List<SchemaField>, Schema> schema = schemaCache.getIfPresent(key);
        if (schema == null) {
            List<SchemaField> rakamSchema = metastore.getCollection(project, collection);

            schema = new SimpleImmutableEntry<>(rakamSchema, convertAvroSchema(rakamSchema));
            schemaCache.put(key, schema);
        }
        Schema avroSchema = schema.getValue();
        List<SchemaField> rakamSchema = schema.getKey();

        GenericData.Record record = new GenericData.Record(avroSchema);
        Set<SchemaField> newFields = null;

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            Schema.Field field = avroSchema.getField(fieldName);

            jp.nextToken();

            if (field == null) {
                fieldName = fieldName.toLowerCase(Locale.ENGLISH);
                field = avroSchema.getField(fieldName);
                if(field == null) {
                    FieldType type = getType(jp);
                    if (type != null) {
                        if (newFields == null)
                            newFields = new HashSet<>();

                        if(fieldName.equals("_user")) {
                            // the type of magic _user field must always be a string
                            // for consistency.
                            type = FieldType.STRING;
                        }
                        SchemaField newField = new SchemaField(fieldName, type);
                        newFields.add(newField);

                        avroSchema = createNewSchema(avroSchema, newField);
                        field = avroSchema.getField(newField.getName());

                        GenericData.Record newRecord = new GenericData.Record(avroSchema);
                        for (Schema.Field f : record.getSchema().getFields()) {
                            newRecord.put(f.name(), record.get(f.name()));
                        }
                        record = newRecord;

                        if(type.isArray() || type.isMap()) {
                            // if the type of new field is ARRAY, we already switched to next token
                            // so current token is not START_ARRAY.
                            record.put(field.pos(), getValue(jp, type, field.schema(), true));
                        } else {
                            record.put(field.pos(), getValue(jp, type, field.schema(), false));
                        }
                        continue;

                    } else {
                        // the type is null or an empty array
                        continue;
                    }
                }
            } else {
                if(field.schema().getType() == NULL) {
                    // TODO: get rid of this loop.
                    for (SchemaField schemaField : conditionalMagicFields.get(fieldName)) {
                        if(avroSchema.getField(schemaField.getName()) == null) {
                            if(newFields == null) {
                                newFields = new HashSet<>();
                            }
                            newFields.add(schemaField);
                        }
                    }
                }
            }

            Object value = getValue(jp, field.schema().getType() == NULL ? null : rakamSchema.get(field.pos()).getType(), field.schema(), false);
            record.put(field.pos(), value);
        }

        if (newFields != null) {
            List<SchemaField> newSchema = metastore.getOrCreateCollectionFieldList(project, collection, newFields);
            Schema newAvroSchema = convertAvroSchema(newSchema);

            schemaCache.put(key, new SimpleImmutableEntry<>(newSchema, newAvroSchema));
            GenericData.Record newRecord = new GenericData.Record(newAvroSchema);
            for (Schema.Field field : record.getSchema().getFields()) {
                newRecord.put(field.name(), record.get(field.name()));
            }
            record = newRecord;
        }

        return new SimpleImmutableEntry<>(rakamSchema, record);
    }

    private Schema createNewSchema(Schema currentSchema, SchemaField newField) {
        List<Schema.Field> avroFields = currentSchema.getFields().stream()
                .filter(field -> field.schema().getType() != Schema.Type.NULL)
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()))
                .collect(toList());
        try {
            avroFields.add(AvroUtil.generateAvroSchema(newField));
        } catch (SchemaParseException e) {
            throw new RakamException("Couldn't create new column: "+e.getMessage(), BAD_REQUEST);
        }

        conditionalMagicFields.keySet().stream()
                .filter(s -> !avroFields.stream().anyMatch(af -> af.name().equals(s)))
                .map(n -> new Schema.Field(n, Schema.create(NULL), "", null))
                .forEach(x -> avroFields.add(x));

        Schema avroSchema = Schema.createRecord("collection", null, null, false);
        avroSchema.setFields(avroFields);

        return avroSchema;
    }

    private Object getValueOfMagicField(JsonParser jp) throws IOException {
        switch (jp.getCurrentToken()) {
            case VALUE_TRUE:
                return Boolean.TRUE;
            case VALUE_FALSE:
                return Boolean.FALSE;
            case VALUE_NUMBER_FLOAT:
                return jp.getValueAsDouble();
            case VALUE_NUMBER_INT:
                return jp.getValueAsLong();
            case VALUE_STRING:
                return jp.getValueAsString();
            case VALUE_NULL:
                return null;
            default:
                throw new RakamException("The value of magic field is unknown", BAD_REQUEST);
        }
    }

    private Object getValue(JsonParser jp, FieldType type, Schema schema, boolean passInitialToken) throws IOException {
        if(type == null) {
            return getValueOfMagicField(jp);
        }

        switch (type) {
            case STRING:
                // TODO: is it a good idea to cast the value automatically?
//                if (t == JsonToken.VALUE_STRING)
                    return jp.getValueAsString();
            case BOOLEAN:
                return jp.getValueAsBoolean();
            case LONG:
                return jp.getValueAsLong();
            case TIME:
                return (long) LocalTime.parse(jp.getValueAsString()).get(ChronoField.MILLI_OF_DAY);
            case DOUBLE:
                return jp.getValueAsDouble();
            case TIMESTAMP:
                if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) {
                    return jp.getValueAsLong();
                }
                try {
                    return Instant.parse(jp.getValueAsString()).toEpochMilli();
                } catch (DateTimeParseException e) {
                    return null;
                }
            case DATE:
                try {
                    return Ints.checkedCast(LocalDate.parse(jp.getValueAsString()).toEpochDay());
                } catch (DateTimeParseException e) {
                    return null;
                }
            default:
                Schema actualSchema = schema.getTypes().get(1);
                if (type.isMap()) {
                    JsonToken t = jp.getCurrentToken();

                    Map<String, Object> map = new HashMap<>();

                    Schema elementType = actualSchema.getValueType();

                    if(!passInitialToken) {
                        if(t != JsonToken.START_OBJECT) {
                            return null;
                        } else {
                            t = jp.nextToken();
                        }
                    } else {
                        // In order to determine the value type of map, getType method performed an extra
                        // jp.nextToken() so the cursor should be at VALUE_STRING token.
                        String key = jp.getParsingContext().getCurrentName();
                        map.put(key, getValue(jp, type.getMapValueType(), elementType, false));
                        t = jp.nextToken();
                    }

                    for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
                        String key = jp.getCurrentName();

                        jp.nextToken();
                        map.put(key, getValue(jp, type.getMapValueType(), elementType, false));
                    }
                    return map;
                }
                if (type.isArray()) {
                    JsonToken t = jp.getCurrentToken();
                    // if the passStartArrayToken is true, we already performed jp.nextToken
                    // so there is no need to check if the current token is
                    if(!passInitialToken) {
                        if(t != JsonToken.START_ARRAY) {
                            return null;
                        } else {
                            t = jp.nextToken();
                        }
                    }

                    Schema elementType = actualSchema.getElementType();

                    List<Object> objects = new ArrayList<>();
                    for (; t != JsonToken.END_ARRAY; t = jp.nextToken()) {
                        objects.add(getValue(jp, type.getArrayElementType(), elementType, false));
                    }
                    return new GenericData.Array(actualSchema, objects);
                }
                throw new JsonMappingException(format("type is not supported."));
        }
    }

    private FieldType getType(JsonParser jp) throws IOException {
        switch (jp.getCurrentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                String value = jp.getValueAsString();
                if (DATE_PATTERN.matcher(value).matches()) {
                    return FieldType.DATE;
                }
                if (TIMESTAMP_PATTERN.matcher(value).matches()) {
                    return FieldType.TIMESTAMP;
                }
//                if (TIME_PATTERN.matcher(value).matches()) {
//                    return FieldType.TIME;
//                }
                return FieldType.STRING;
            case VALUE_FALSE:
                return FieldType.BOOLEAN;
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                return FieldType.DOUBLE;
            case VALUE_TRUE:
                return FieldType.BOOLEAN;
            case START_ARRAY:
                JsonToken t = jp.nextToken();
                if(t == JsonToken.END_ARRAY) {
                    // if the array is null, return null as value.
                    // TODO: if the key already has a type, return that type instead of null.
                    return null;
                }
                return getType(jp).convertToArrayType();
            case START_OBJECT:
                t = jp.nextToken();
                if(t == JsonToken.END_OBJECT) {
                    // if the map is null, return null as value.
                    // TODO: if the key already has a type, return that type instead of null.
                    return null;
                }
                if(t != JsonToken.FIELD_NAME) {
                    throw new IllegalArgumentException();
                }
                jp.nextToken();
                return getType(jp).convertToMapValueType();
            default:
                throw new JsonMappingException(format("the type is not supported: %s", jp.getValueAsString()));
        }
    }
}

