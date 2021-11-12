package org.rakam.collection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.Event.EventContext;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.util.*;

import javax.inject.Inject;
import java.io.IOException;
import java.text.Normalizer;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.fasterxml.jackson.core.JsonToken.*;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.avro.Schema.Type.NULL;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.MASTER_KEY;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.analysis.InternalConfig.FIXED_SCHEMA;
import static org.rakam.analysis.InternalConfig.USER_TYPE;
import static org.rakam.collection.FieldType.*;
import static org.rakam.util.AvroUtil.convertAvroSchema;
import static org.rakam.util.ValidationUtil.checkCollectionValid;
import static org.rakam.util.ValidationUtil.stripName;

public class JsonEventDeserializer extends JsonDeserializer<Event> {
    private final Map<String, List<SchemaField>> conditionalMagicFields;
    private final Metastore metastore;
    private final Cache<ProjectCollection, Map.Entry<List<SchemaField>, Schema>> schemaCache =
            CacheBuilder
                    .newBuilder()
                    .expireAfterWrite(30, TimeUnit.MINUTES).build();
    private final Set<SchemaField> constantFields;
    private final ApiKeyService apiKeyService;
    private final ConfigManager configManager;
    private final SchemaChecker schemaChecker;
    private final ProjectConfig projectConfig;
    private final ImmutableMap<String, FieldType> conditionalFieldMapping;
    private final EventStore eventstore;
    private final Set<SchemaField> rakamInvalidSchema;
    private final Schema rakamInvalidAvroSchema;

    @Inject
    public JsonEventDeserializer(Metastore metastore,
                                 ApiKeyService apiKeyService,
                                 ConfigManager configManager,
                                 SchemaChecker schemaChecker,
                                 ProjectConfig projectConfig,
                                 EventStore eventstore,
                                 FieldDependency fieldDependency) {
        this.metastore = metastore;
        this.conditionalMagicFields = fieldDependency.dependentFields;
        this.apiKeyService = apiKeyService;
        this.schemaChecker = schemaChecker;
        this.projectConfig = projectConfig;
        this.eventstore = eventstore;
        this.configManager = configManager;
        this.constantFields = fieldDependency.constantFields;
        conditionalFieldMapping = conditionalMagicFields.values().stream()
                .flatMap(e -> e.stream()).collect(toImmutableMap(e -> e.getName(), e -> e.getType()));
        this.rakamInvalidSchema = Sets.union(ImmutableSet.of(
                new SchemaField("collection", STRING),
                new SchemaField("property", STRING),
                new SchemaField("type", STRING),
                new SchemaField("event_id", STRING),
                new SchemaField("error_message", STRING),
                new SchemaField("encoded_value", STRING),
                new SchemaField("_user", STRING)), constantFields);
        this.rakamInvalidAvroSchema = AvroUtil.convertAvroSchema(rakamInvalidSchema);
    }

    public static Object getValueOfMagicField(JsonParser jp)
            throws IOException {
        switch (jp.getCurrentToken()) {
            case VALUE_TRUE:
                return TRUE;
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

    private static FieldType getTypeForUnknown(JsonParser jp)
            throws IOException {
        switch (jp.getCurrentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                String value = jp.getValueAsString();

                try {
                    DateTimeUtils.parseDate(value);
                    return FieldType.DATE;
                } catch (Exception e) {

                }

                try {
                    DateTimeUtils.parseTimestamp(value);
                    return FieldType.TIMESTAMP;
                } catch (Exception e) {

                }

                return STRING;
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

                FieldType type;
                if (t.isScalarValue()) {
                    type = getTypeForUnknown(jp);
                } else {
                    type = MAP_STRING;
                }
                if (type == null) {
                    // TODO: what if the other values are not null?
                    while (t != END_ARRAY) {
                        if (!t.isScalarValue()) {
                            return ARRAY_STRING;
                        } else {
                            t = jp.nextToken();
                        }
                    }
                    return null;
                }
                if (type.isArray() || type.isMap()) {
                    return ARRAY_STRING;
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
                t = jp.nextToken();
                if (!t.isScalarValue()) {
                    return MAP_STRING;
                }
                type = getTypeForUnknown(jp);
                if (type == null) {
                    // TODO: what if the other values are not null?
                    while (t != END_OBJECT) {
                        if (!t.isScalarValue()) {
                            return MAP_STRING;
                        } else {
                            t = jp.nextToken();
                        }
                    }
                    jp.nextToken();

                    return null;
                }

                if (type.isArray() || type.isMap()) {
                    return MAP_STRING;
                }
                return type.convertToMapValueType();
            default:
                throw new JsonMappingException(jp, format("The type is not supported: %s", jp.getValueAsString()));
        }
    }

    @Override
    public Event deserialize(JsonParser jp, DeserializationContext ctx)
            throws IOException {
        Object project = ctx.getAttribute("project");
        Object masterKey = ctx.getAttribute("master_key");
        return deserializeWithProject(
                jp,
                project != null ? project.toString() : null,
                null,
                Boolean.TRUE.equals(masterKey));
    }

    public Event deserializeWithProject(JsonParser jp, String project, EventContext mainApi, boolean masterKey)
            throws IOException, RakamException {
        Map.Entry<List<SchemaField>, GenericData.Record> properties = null;
        String collection = null;

        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }

        TokenBuffer propertiesBuffer = null;
        EventContext api = null;
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            t = jp.nextToken();

            switch (fieldName) {
                case "collection":
                    if (t != VALUE_STRING) {
                        throw new RakamException("collection parameter must be a string", BAD_REQUEST);
                    }
                    String collectionStr = jp.getValueAsString().toLowerCase();
                    if(collectionStr.isEmpty()) {
                        collectionStr = "(empty string)";
                    }

                    collection = checkCollectionValid(collectionStr);
                    break;
                case "event_id":
                    if (t != VALUE_NUMBER_INT) {
                        throw new RakamException("event_id must be numeric", BAD_REQUEST);
                    }
                    break;
                case "api":
                    if (api != null) {
                        throw new RakamException("api is already set", BAD_REQUEST);
                    }
                    api = jp.readValueAs(EventContext.class);
                    break;
                case "properties":
                    t = jp.getCurrentToken();
                    if (t != START_OBJECT) {
                        throw new RakamException("properties must be an object", BAD_REQUEST);
                    }

                    if (collection == null || api == null) {
                        propertiesBuffer = jp.readValueAs(TokenBuffer.class);
                    } else {
                        if (project == null) {
                            if (api.apiKey == null) {
                                throw new RakamException("api.api_key is required", BAD_REQUEST);
                            }
                            try {
                                project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
                            } catch (RakamException e) {
                                try {
                                    project = apiKeyService.getProjectOfApiKey(api.apiKey, MASTER_KEY);
                                } catch (Exception e1) {
                                    if (e.getStatusCode() == FORBIDDEN) {
                                        throw new RakamException("api_key is invalid", FORBIDDEN);
                                    }

                                    throw e;
                                }
                                masterKey = true;
                            }
                        }

                        if (collection == null) {
                            throw new RakamException("Collection is not set.", BAD_REQUEST);
                        }

                        properties = parseProperties(project, collection, jp, masterKey);

                        t = jp.getCurrentToken();

                        if (t != END_OBJECT) {
                            if (t == JsonToken.START_OBJECT) {
                                throw new RakamException("Nested properties are not supported.", BAD_REQUEST);
                            } else {
                                throw new RakamException("Error while de-serializing event", INTERNAL_SERVER_ERROR);
                            }
                        }
                    }
                    break;
                default:
                    throw new RakamException(String.format("Unrecognized field '%s'. Should be one of (api, collection, properties)", fieldName), BAD_REQUEST);
            }
        }
        if (properties == null) {
            if (propertiesBuffer != null) {
                if (project == null) {
                    if (api == null) {
                        throw new RakamException("api parameter is required", BAD_REQUEST);
                    }
                    try {
                        project = apiKeyService.getProjectOfApiKey(api.apiKey, WRITE_KEY);
                    } catch (RakamException e) {
                        try {
                            project = apiKeyService.getProjectOfApiKey(api.apiKey, MASTER_KEY);
                        } catch (RakamException e1) {
                            if (e1.getStatusCode() == FORBIDDEN) {
                                throw new RakamException("write_key or master_key is invalid.", FORBIDDEN);
                            }
                        }
                        masterKey = true;
                    }
                }
                JsonParser fakeJp = propertiesBuffer.asParser(jp);
                // pass START_OBJECT
                fakeJp.nextToken();
                properties = parseProperties(project, collection, fakeJp, masterKey);
            } else {
                throw new JsonMappingException(jp, "properties is null");
            }
        }
        return new Event(project, collection, api == null ? mainApi : api, properties.getKey(), properties.getValue());
    }

    public Map.Entry<List<SchemaField>, GenericData.Record> parseProperties(String project, String collection, JsonParser jp, boolean masterKey)
            throws IOException, NotExistsException {
        ProjectCollection key = new ProjectCollection(project, collection);
        Map.Entry<List<SchemaField>, Schema> schema = schemaCache.getIfPresent(key);
        boolean isNew = schema == null;
        if (schema == null) {
            List<SchemaField> rakamSchema = metastore.getCollection(project, collection);
            isNew = rakamSchema == null || rakamSchema.isEmpty();
            rakamSchema = isNew ? ImmutableList.copyOf(constantFields) : rakamSchema;
            schema = new SimpleImmutableEntry<>(rakamSchema, convertAvroSchema(rakamSchema, conditionalMagicFields));
            schemaCache.put(key, schema);
        }

        InvalidSchemaLogger invalidSchemaLogger = new InvalidSchemaLogger(project, collection);

        Schema avroSchema = schema.getValue();
        List<SchemaField> rakamSchema = schema.getKey();

        GenericData.Record record = new GenericData.Record(avroSchema);
        List<SchemaField> newFields = null;

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName;
            try {
                fieldName = stripName(jp.getCurrentName(), "field name");
            } catch (IllegalArgumentException e) {
                fieldName = stripName(Normalizer.normalize(jp.getCurrentName(), Normalizer.Form.NFD)
                        .replaceAll("\\p{InCombiningDiacriticalMarks}+", ""), "field name");
            }

            Schema.Field field = avroSchema.getField(fieldName);

            jp.nextToken();

            if (field == null) {
                FieldType type = conditionalFieldMapping.get(fieldName);
                if (type == null) {
                    type = getTypeForUnknown(jp);
                }
                if (type != null) {
                    if (newFields == null) {
                        newFields = new ArrayList<>();
                    }

                    if (fieldName.equals(projectConfig.getUserColumn())) {
                        // the type of magic _user field must be consistent between collections
                        if (type.isArray() || type.isMap()) {
                            throw new RakamException("_user field must be numeric or string.", BAD_REQUEST);
                        }
                        final FieldType eventUserType = type.isNumeric() ? (type != INTEGER ? LONG : INTEGER) : STRING;
                        type = configManager.setConfigOnce(project, USER_TYPE.name(), eventUserType);
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


                    Object value = null;
                    try {
                        value = getValue(jp, type, field,
                                // if the type of new field is ARRAY, we already switched to next token
                                // so current token is not START_ARRAY.
                                type.isArray() || type.isMap());
                    } catch (ParseException e) {
                        invalidSchemaLogger.log(fieldName, type, e.getMessage(), e.value);
                    }

                    record.put(field.pos(), value);
                    continue;
                } else {
                    // the type is null or an empty array
                    t = jp.getCurrentToken();
                    continue;
                }
            } else {
                if (field.schema().getType() == NULL) {
                    // TODO: if there is a field in the database with this name, the code will add the types
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

            Object value = null;
            try {
                value = getValue(jp, type, field, false);
            } catch (ParseException e) {
                invalidSchemaLogger.log(fieldName, type, e.getMessage(), e.value);
            }
            record.put(field.pos(), value);
        }

        if (isNew && newFields == null) {
            newFields = new ArrayList<>();
        }

        if (newFields != null) {
            if (!masterKey && TRUE.equals(configManager.getConfig(project, FIXED_SCHEMA.name(), Boolean.class))) {
                throw new RakamException("Schema is invalid", BAD_REQUEST);
            }

            if (isNew) {
                if (!newFields.stream().anyMatch(e -> e.getName().equals("_user"))) {
                    newFields.add(new SchemaField("_user", configManager.setConfigOnce(project, USER_TYPE.name(), STRING)));
                }
            }

            rakamSchema = metastore.getOrCreateCollectionFields(project, collection,
                    schemaChecker.checkNewFields(collection, ImmutableSet.copyOf(newFields)));
            Schema newAvroSchema = convertAvroSchema(rakamSchema, conditionalMagicFields);

            schemaCache.put(key, new SimpleImmutableEntry<>(rakamSchema, newAvroSchema));
            GenericData.Record newRecord = new GenericData.Record(newAvroSchema);

            for (Schema.Field field : record.getSchema().getFields()) {
                Object value = record.get(field.name());
                newRecord.put(field.name(), value);
            }
            record = newRecord;
        }

        invalidSchemaLogger.flushInBackground(record.get("_id"), record.get(projectConfig.getUserColumn()), record.get(projectConfig.getTimeColumn()));

        return new SimpleImmutableEntry<>(rakamSchema, record);
    }

    private Schema createNewSchema(Schema currentSchema, SchemaField newField) {
        List<Schema.Field> avroFields = currentSchema.getFields().stream()
                .filter(field -> field.schema().getType() != Schema.Type.NULL)
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()))
                .collect(toList());
        try {
            avroFields.add(AvroUtil.generateAvroField(newField));
        } catch (SchemaParseException e) {
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

    private Object getValue(JsonParser jp, FieldType type, Schema.Field field, boolean passInitialToken)
            throws IOException, ParseException {
        if (type == null) {
            return getValueOfMagicField(jp);
        }

        if (jp.getCurrentToken().isScalarValue() && !passInitialToken) {
            if (jp.getCurrentToken() == VALUE_NULL) {
                return null;
            }

            switch (type) {
                case STRING:
                    String valueAsString = jp.getValueAsString();
                    if (valueAsString.length() > projectConfig.getMaxStringLength()) {
                        return valueAsString.substring(0, projectConfig.getMaxStringLength());
                    }
                    return valueAsString;
                case BOOLEAN:
                    if (jp.getCurrentToken() == VALUE_FALSE || jp.getCurrentToken() == VALUE_TRUE) {
                        return jp.getValueAsBoolean();
                    } else {
                        if(jp.getCurrentToken() == VALUE_STRING) {
                            if(jp.getValueAsString().toLowerCase(Locale.ENGLISH).equals("true")) {
                                return true;
                            }
                            if(jp.getValueAsString().toLowerCase(Locale.ENGLISH).equals("false")) {
                                return false;
                            }
                        }
                        throw new ParseException(String.format("Invalid value %s", jp.getCurrentToken()), jp.readValueAsTree());
                    }
                case LONG:
                case DECIMAL:
                case INTEGER:
                case DOUBLE:
                    if (jp.getCurrentToken() == VALUE_NUMBER_FLOAT || jp.getCurrentToken() == VALUE_NUMBER_INT) {
                        if(type == INTEGER) {
                            return jp.getValueAsInt();
                        }
                        if(type == DOUBLE) {
                            return jp.getValueAsDouble();
                        }
                        if(type == LONG || type == DECIMAL) {
                            return jp.getValueAsLong();
                        }
                    } else {
                        if(jp.getCurrentToken() == VALUE_STRING) {
                            if(jp.getValueAsString().isEmpty()) {
                                return null;
                            }

                            if(type == INTEGER) {
                                int valueAsInt = jp.getValueAsInt(Integer.MIN_VALUE);
                                if(valueAsInt != Integer.MIN_VALUE) {
                                    return valueAsInt;
                                }
                            }
                            if(type == DOUBLE) {
                                double valueAsDouble = jp.getValueAsDouble(Double.MIN_VALUE);
                                if(valueAsDouble != Double.MIN_VALUE) {
                                    return valueAsDouble;
                                }
                            }
                            if(type == LONG || type == DECIMAL) {
                                long valueAsLong = jp.getValueAsLong(Long.MIN_VALUE);
                                if(valueAsLong != Long.MIN_VALUE) {
                                    return valueAsLong;
                                }
                            }
                            // try to parse the value
                        }

                        throw new ParseException(String.format("Invalid token %s", jp.getCurrentToken()), jp.readValueAsTree());
                    }
                case TIME:
                    try {
                        return (long) LocalTime.parse(jp.getValueAsString())
                                .get(ChronoField.MILLI_OF_DAY);
                    } catch (Exception e) {
                        throw new ParseException(e.getMessage(), jp.readValueAsTree());
                    }
                case TIMESTAMP:
                    if (jp.getCurrentToken().isNumeric()) {
                        return jp.getValueAsLong();
                    }

                    try {
                        return DateTimeUtils.parseTimestamp(jp.getValueAsString());
                    } catch (Exception e) {
                        if (field.name().equals(projectConfig.getTimeColumn())) {
                            throw new RakamException(String.format("Unable to parse TIMESTAMP value '%s' in time column", jp.getValueAsString()),
                                    BAD_REQUEST);
                        }
                        throw new ParseException(e.getMessage(), jp.readValueAsTree());
                    }
                case DATE:
                    try {
                        return DateTimeUtils.parseDate(jp.getValueAsString());
                    } catch (Exception e) {
                        if (field.name().equals(projectConfig.getTimeColumn())) {
                            throw new RakamException(String.format("Unable to parse DATE value '%s' in time column", jp.getValueAsString()),
                                    BAD_REQUEST);
                        }
                        throw new ParseException(e.getMessage(), jp.readValueAsTree());
                    }
                default:
                    if (type.isArray()) {
                        if(jp.currentToken() == VALUE_STRING && jp.getValueAsString().trim().startsWith("[")) {
                            ObjectMapper mapper = JsonHelper.getMapper();
                            JsonNode parsedJson = null;
                            try {
                                // try to parse json
                                parsedJson = mapper.readTree(jp.getValueAsString());
                            } catch (Exception e) {
                                //
                            }

                            if(parsedJson != null) {
                                // parse as json
                                JsonParser arrayJp = mapper.treeAsTokens(parsedJson);
                                arrayJp.nextToken();
                                Object value = getValue(arrayJp, type, field, false);
                                if (value != null) {
                                    return value;
                                }
                            }
                        }

                        Object value = getValue(jp, type.getArrayElementType(), null, false);
                        if (value != null) {
                            Schema actualSchema = field.schema().getTypes().get(1);
                            return new GenericData.Array(actualSchema, ImmutableList.of(value));
                        }
                    }

                    throw new ParseException("Unable to cast scalar value to a complex type", jp.readValueAsTree());
            }
        } else {
            Schema actualSchema = field.schema().getTypes().get(1);
            if (type.isMap()) {
                JsonToken t = jp.getCurrentToken();

                Map<String, Object> map = new HashMap<>();

                if (!passInitialToken) {
                    if (t != JsonToken.START_OBJECT) {
                        throw new ParseException(String.format("Unable to parse token %s", t), jp.readValueAsTree());
                    } else {
                        t = jp.nextToken();
                    }
                } else {
                    // In order to determine the value type of map, getTypeForUnknown method performed an extra
                    // jp.nextToken() so the cursor should be at VALUE_STRING token.
                    String key = jp.getCurrentName();
                    Object value;
                    if (t.isScalarValue()) {
                        value = getValue(jp, type.getMapValueType(), null, false);
                    } else {
                        value = JsonHelper.encode(jp.readValueAsTree());
                    }
                    map.put(key, value);
                    t = jp.nextToken();
                }

                for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
                    String key = jp.getCurrentName();

                    try {
                        Object value;
                        if (!jp.nextToken().isScalarValue()) {
                            throw new ParseException("Unable to cast scalar value to a complex type", jp.readValueAsTree());
                        } else {
                            value = getValue(jp, type.getMapValueType(), null, false);
                        }

                        map.put(key, value);
                    } catch (ParseException e) {
                        // ignore field
                    }
                }
                return map;
            } else if (type.isArray()) {
                JsonToken t = jp.getCurrentToken();
                // if the passStartArrayToken is true, we already performed jp.nextToken
                // so there is no need to check if the current token is START_ARRAY
                if (!passInitialToken) {
                    if (t != JsonToken.START_ARRAY) {
                        throw new ParseException("Unable to cast scalar value to a complex type", jp.readValueAsTree());
                    } else {
                        t = jp.nextToken();
                    }
                }

                List<Object> objects = new ArrayList<>();
                for (; t != JsonToken.END_ARRAY; t = jp.nextToken()) {
                    try {
                        if (!t.isScalarValue()) {
                            if (type.getArrayElementType() != STRING) {
                                throw new ParseException("Nested properties are not supported if the type is not MAP_STRING.", jp.readValueAsTree());
                            }

                            objects.add(JsonHelper.encode(jp.readValueAsTree()));
                        } else {
                            objects.add(getValue(jp, type.getArrayElementType(), null, false));
                        }
                    } catch (ParseException e) {
                        // ignore field, TODO: add it to invalid schema as well
                    }
                }
                return new GenericData.Array(actualSchema, objects);
            } else {
                TreeNode value = jp.readValueAsTree();
                if(value.isArray() && value.size() == 0) {
                    return null;
                }
                if(value.isObject() && !value.fieldNames().hasNext()) {
                    return null;
                }
                throw new ParseException("Unable to cast complex value to a scalar type", value);
            }
        }
    }

    @VisibleForTesting
    public void cleanCache() {
        schemaCache.invalidateAll();
    }

    public class InvalidSchemaLogger {
        private final String project;
        private final String collection;
        private List<Event> events = null;

        public InvalidSchemaLogger(String project, String collection) {
            this.project = project;
            this.collection = collection;
        }

        public void log(String name, FieldType type, String error, Object value) {
            List<SchemaField> fields = metastore.getOrCreateCollectionFields(project, "$invalid_schema", rakamInvalidSchema);

            if(events == null) {
                events = new ArrayList<>();
            }

            GenericData.Record record = new GenericData.Record(rakamInvalidAvroSchema);
            record.put("collection", this.collection);
            record.put("property", name);
            record.put("type", type.toString());
            record.put("event_id", null);
            record.put("error_message", error);
            record.put("encoded_value", JsonHelper.encode(value));
            events.add(new Event(project, "$invalid_schema", EventContext.empty(), fields, record));
        }

        public void flushInBackground(Object eventId, Object user, Object time) {
            if(events != null) {
                if(eventId != null) {
                    String eventIdValue = eventId.toString();
                    events.forEach(event -> {
                        event.properties().put("event_id",  eventIdValue);
                        event.properties().put(projectConfig.getUserColumn(),  user);
                        event.properties().put(projectConfig.getTimeColumn(),  time);
                    });
                }
                eventstore.storeBatch(events);
            }
        }
    }

    public static class ParseException extends Exception {
        private final Object value;

        public ParseException(String message, Object value) {
            super(message);
            this.value = value;
        }

        // Stack traces are expensive and we don't need them.
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }
}

