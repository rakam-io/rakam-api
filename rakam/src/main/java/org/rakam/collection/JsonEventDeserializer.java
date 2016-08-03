package org.rakam.collection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InternalConfig;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event.EventContext;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.util.AvroUtil;
import org.rakam.util.DateTimeUtils;
import org.rakam.util.NotExistsException;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.avro.Schema.Type.NULL;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.analysis.InternalConfig.USER_TYPE;
import static org.rakam.collection.SchemaField.stripName;
import static org.rakam.util.AvroUtil.convertAvroSchema;

public class JsonEventDeserializer
        extends JsonDeserializer<Event>
{
    private final Map<String, List<SchemaField>> conditionalMagicFields;
    private final Metastore metastore;
    private final Cache<ProjectCollection, Map.Entry<List<SchemaField>, Schema>> schemaCache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES).build();
    private final Set<SchemaField> constantFields;
    private final ApiKeyService apiKeyService;
    private final ConfigManager configManager;

    @Inject
    public JsonEventDeserializer(Metastore metastore,
            ApiKeyService apiKeyService,
            ConfigManager configManager,
            FieldDependency fieldDependency)
    {
        this.metastore = metastore;
        this.conditionalMagicFields = fieldDependency.dependentFields;
        this.apiKeyService = apiKeyService;
        this.configManager = configManager;
        this.constantFields = fieldDependency.constantFields;
    }

    @Override
    public Event deserialize(JsonParser jp, DeserializationContext ctx)
            throws IOException
    {
        return deserializeWithProject(jp, null, null);
    }

    public Event deserializeWithProject(JsonParser jp, String project, EventContext api)
            throws IOException, RakamException
    {
        Map.Entry<List<SchemaField>, GenericData.Record> properties = null;
        String collection = null;

        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        TokenBuffer propertiesBuffer = null;
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            t = jp.nextToken();

            switch (fieldName) {
                case "collection":
                    if (t != VALUE_STRING) {
                        throw new RakamException("collection parameter must be a string", BAD_REQUEST);
                    }
                    collection = jp.getValueAsString().toLowerCase();
                    break;
                case "api":
                    if (api != null) {
                        throw new RakamException("api is already set", BAD_REQUEST);
                    }
                    api = jp.readValueAs(EventContext.class);
                    break;
                case "properties":
                    if (collection == null) {
                        propertiesBuffer = jp.readValueAs(TokenBuffer.class);
                    }
                    else {
                        if (project == null) {
                            project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
                        }
                        properties = parseProperties(project, collection, jp);

                        t = jp.getCurrentToken();

                        if (t != JsonToken.END_OBJECT) {
                            if (t == JsonToken.START_OBJECT) {
                                throw new RakamException("Nested properties are not supported", BAD_REQUEST);
                            }
                            else {
                                throw new RakamException("Error while de-serializing event", INTERNAL_SERVER_ERROR);
                            }
                        }
                    }
                    break;
                default:
                    throw new RakamException(String.format("Unrecognized field '%s' ", fieldName), BAD_REQUEST);
            }
        }
        if (properties == null) {
            if(propertiesBuffer != null) {
                if (project == null) {
                    project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
                }
                properties = parseProperties(project, collection, propertiesBuffer.asParser(jp));
            } else {
                throw new JsonMappingException("properties is null");
            }
        }
        return new Event(project, collection, api, properties.getKey(), properties.getValue());
    }

    private Map.Entry<List<SchemaField>, GenericData.Record> parseProperties(String project, String collection, JsonParser jp)
            throws IOException, NotExistsException
    {
        ProjectCollection key = new ProjectCollection(project, collection);
        Map.Entry<List<SchemaField>, Schema> schema = schemaCache.getIfPresent(key);
        if (schema == null) {
            List<SchemaField> rakamSchema = metastore.getCollection(project, collection);
            if (rakamSchema.isEmpty()) {
                // new collection
                FieldType userType = configManager.setConfigOnce(project, USER_TYPE.name(), FieldType.STRING);
                Set<SchemaField> fields = ImmutableSet.<SchemaField>builder()
                        .addAll(constantFields)
                        .add(new SchemaField("_user", userType))
                        .build();

                if (collection.startsWith("$")) {
                    throw new RakamException("Collection names cannot start with $.", BAD_REQUEST);
                }

                rakamSchema = metastore.getOrCreateCollectionFieldList(project, collection, fields);
            }

            schema = new SimpleImmutableEntry<>(rakamSchema, convertAvroSchema(rakamSchema, conditionalMagicFields));
            schemaCache.put(key, schema);
        }

        Schema avroSchema = schema.getValue();
        List<SchemaField> rakamSchema = schema.getKey();

        GenericData.Record record = new GenericData.Record(avroSchema);
        List<SchemaField> newFields = null;

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            Schema.Field field = avroSchema.getField(fieldName);

            jp.nextToken();

            if (field == null) {
                field = avroSchema.getField(stripName(fieldName));

                if (field == null) {
                    FieldType type = getType(jp);
                    if (type != null) {
                        if (newFields == null) {
                            newFields = new ArrayList<>();
                        }

                        if (fieldName.equals("_user")) {
                            // the type of magic _user field must be consistent between collections
                            if (type.isArray() || type.isMap()) {
                                throw new RakamException("_user field must be numeric or string.", BAD_REQUEST);
                            }
                            final FieldType eventUserType = type.isNumeric() ? (type != FieldType.INTEGER ? FieldType.LONG : FieldType.INTEGER) :
                                    FieldType.STRING;
                            type = configManager.setConfigOnce(project, USER_TYPE.name(), eventUserType);
                        }

                        SchemaField newField = new SchemaField(fieldName, type);
                        newFields.add(newField);

                        avroSchema = createNewSchema(project, avroSchema, newField);
                        field = avroSchema.getField(newField.getName());

                        GenericData.Record newRecord = new GenericData.Record(avroSchema);
                        for (Schema.Field f : record.getSchema().getFields()) {
                            newRecord.put(f.name(), record.get(f.name()));
                        }
                        record = newRecord;

                        if (type.isArray() || type.isMap()) {
                            // if the type of new field is ARRAY, we already switched to next token
                            // so current token is not START_ARRAY.
                            record.put(field.pos(), getValue(jp, type, field, true));
                        }
                        else {
                            record.put(field.pos(), getValue(jp, type, field, false));
                        }
                        continue;
                    }
                    else {
                        // the type is null or an empty array
                        continue;
                    }
                }
            }
            else {
                if (field.schema().getType() == NULL) {
                    // TODO: get rid of this loop.
                    for (SchemaField schemaField : conditionalMagicFields.get(fieldName)) {
                        if (avroSchema.getField(schemaField.getName()) == null) {
                            if (newFields == null) {
                                newFields = new ArrayList<>();
                            }
                            newFields.add(schemaField);
                        }
                    }
                }
            }

            FieldType type = field.schema().getType() == NULL ? null :
                    (field.pos() >= rakamSchema.size() ?
                            newFields.get(field.pos() - rakamSchema.size()) : rakamSchema.get(field.pos())).getType();
            Object value = getValue(jp, type, field, false);
            record.put(field.pos(), value);
        }

        if (newFields != null) {
            rakamSchema = metastore.getOrCreateCollectionFieldList(project, collection, ImmutableSet.copyOf(newFields));
            Schema newAvroSchema = convertAvroSchema(rakamSchema, conditionalMagicFields);

            schemaCache.put(key, new SimpleImmutableEntry<>(rakamSchema, newAvroSchema));
            GenericData.Record newRecord = new GenericData.Record(newAvroSchema);

            for (Schema.Field field : record.getSchema().getFields()) {
                Object value = record.get(field.name());
                newRecord.put(field.name(), value);
            }
            record = newRecord;
        }

        return new SimpleImmutableEntry<>(rakamSchema, record);
    }

    private Schema createNewSchema(String project, Schema currentSchema, SchemaField newField)
    {
        if (Boolean.TRUE == configManager.getConfig(project, InternalConfig.FIXED_SCHEMA.name(), Boolean.class)) {
            throw new RakamException(BAD_REQUEST);
        }
        List<Schema.Field> avroFields = currentSchema.getFields().stream()
                .filter(field -> field.schema().getType() != Schema.Type.NULL)
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()))
                .collect(toList());
        try {
            avroFields.add(AvroUtil.generateAvroField(newField));
        }
        catch (SchemaParseException e) {
            throw new RakamException("Couldn't create new column: " + e.getMessage(), BAD_REQUEST);
        }

        conditionalMagicFields.keySet().stream()
                .filter(s -> !avroFields.stream().anyMatch(af -> af.name().equals(s)))
                .map(n -> new Schema.Field(n, Schema.create(NULL), "", null))
                .forEach(x -> avroFields.add(x));

        Schema avroSchema = Schema.createRecord("collection", null, null, false);
        avroSchema.setFields(avroFields);

        return avroSchema;
    }

    public static Object getValueOfMagicField(JsonParser jp)
            throws IOException
    {
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

    private static Object getValue(JsonParser jp, FieldType type, Schema.Field field, boolean passInitialToken)
            throws IOException
    {
        if (type == null) {
            return getValueOfMagicField(jp);
        }

        switch (type) {
            case STRING:
                return jp.getValueAsString();
            case BOOLEAN:
                return jp.getValueAsBoolean();
            case LONG:
            case DECIMAL:
                return jp.getValueAsLong();
            case INTEGER:
                return jp.getValueAsInt();
            case TIME:
                return (long) LocalTime.parse(jp.getValueAsString()).get(ChronoField.MILLI_OF_DAY);
            case DOUBLE:
                return jp.getValueAsDouble();
            case TIMESTAMP:
                if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) {
                    return jp.getValueAsLong();
                }
                try {
                    return DateTimeUtils.parseTimestamp(jp.getValueAsString());
                }
                catch (Exception e) {
                    return null;
                }
            case DATE:
                try {
                    return DateTimeUtils.parseDate(jp.getValueAsString());
                }
                catch (Exception e) {
                    return null;
                }
            default:
                Schema actualSchema = field.schema().getTypes().get(1);
                if (type.isMap()) {
                    JsonToken t = jp.getCurrentToken();

                    Map<String, Object> map = new HashMap<>();

                    if (!passInitialToken) {
                        if (t != JsonToken.START_OBJECT) {
                            return null;
                        }
                        else {
                            t = jp.nextToken();
                        }
                    }
                    else {
                        // In order to determine the value type of map, getType method performed an extra
                        // jp.nextToken() so the cursor should be at VALUE_STRING token.
                        String key = jp.getParsingContext().getCurrentName();
                        map.put(key, getValue(jp, type.getMapValueType(), null, false));
                        t = jp.nextToken();
                    }

                    for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
                        String key = jp.getCurrentName();

                        if (!jp.nextToken().isScalarValue()) {
                            throw new JsonMappingException(String.format("Nested properties are not supported. ('%s' field)", field.name()));
                        }

                        map.put(key, getValue(jp, type.getMapValueType(), null, false));
                    }
                    return map;
                }
                if (type.isArray()) {
                    JsonToken t = jp.getCurrentToken();
                    // if the passStartArrayToken is true, we already performed jp.nextToken
                    // so there is no need to check if the current token is
                    if (!passInitialToken) {
                        if (t != JsonToken.START_ARRAY) {
                            return null;
                        }
                        else {
                            t = jp.nextToken();
                        }
                    }

                    List<Object> objects = new ArrayList<>();
                    for (; t != JsonToken.END_ARRAY; t = jp.nextToken()) {
                        if (!t.isScalarValue()) {
                            throw new JsonMappingException(String.format("Nested properties are not supported. ('%s' field)", field.name()));
                        }
                        objects.add(getValue(jp, type.getArrayElementType(), null, false));
                    }
                    return new GenericData.Array(actualSchema, objects);
                }
                throw new JsonMappingException(format("type is not supported."));
        }
    }

    private static FieldType getType(JsonParser jp)
            throws IOException
    {
        switch (jp.getCurrentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                String value = jp.getValueAsString();

                try {
                    DateTimeUtils.parseTimestamp(value);
                    return FieldType.TIMESTAMP;
                }
                catch (IllegalArgumentException e) {

                }

                try {
                    DateTimeUtils.parseDate(value);
                    return FieldType.DATE;
                }
                catch (IllegalArgumentException e) {

                }
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
                if (t == JsonToken.END_ARRAY) {
                    // if the array is null, return null as value.
                    // TODO: if the key already has a type, return that type instead of null.
                    return null;
                }
                FieldType type = getType(jp);
                if (type.isArray() || type.isMap()) {
                    throw new RakamException("Nested properties is not supported", BAD_REQUEST);
                }
                return type.convertToArrayType();
            case START_OBJECT:
                t = jp.nextToken();
                if (t == JsonToken.END_OBJECT) {
                    // if the map is null, return null as value.
                    // TODO: if the key already has a type, return that type instead of null.
                    return null;
                }
                if (t != JsonToken.FIELD_NAME) {
                    throw new IllegalArgumentException();
                }
                jp.nextToken();
                type = getType(jp);

                if (type.isArray() || type.isMap()) {
                    throw new RakamException("Nested properties is not supported", BAD_REQUEST);
                }
                return type.convertToMapValueType();
            default:
                throw new JsonMappingException(format("the type is not supported: %s", jp.getValueAsString()));
        }
    }

    @VisibleForTesting
    public void cleanCache()
    {
        schemaCache.invalidateAll();
    }
}

