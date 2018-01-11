package org.rakam.collection;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.config.ProjectConfig;
import org.rakam.util.DateTimeUtils;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;
import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.rakam.analysis.InternalConfig.USER_TYPE;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.collection.JsonEventDeserializer.getValueOfMagicField;
import static org.rakam.util.AvroUtil.convertAvroSchema;
import static org.rakam.util.AvroUtil.generateAvroSchema;

public class CsvEventDeserializer
        extends JsonDeserializer<EventList> {

    private final Metastore metastore;
    private final Set<SchemaField> constantFields;
    private final ConfigManager configManager;
    private final Map<String, List<SchemaField>> dependentFields;
    private final JsonFactory jsonFactory = new JsonFactory();
    private final SchemaChecker schemaChecker;
    private final ProjectConfig projectConfig;

    @Inject
    public CsvEventDeserializer(
            Metastore metastore,
            ProjectConfig projectConfig,
            ConfigManager configManager,
            SchemaChecker schemaChecker,
            FieldDependency fieldDependency) {
        this.metastore = metastore;
        this.configManager = configManager;
        this.projectConfig = projectConfig;
        this.schemaChecker = schemaChecker;
        this.constantFields = fieldDependency.constantFields;
        this.dependentFields = fieldDependency.dependentFields;
    }

    @Override
    public EventList deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        String project = (String) ctxt.getAttribute("project");
        String collection = (String) ctxt.getAttribute("collection");
        String apiKey = (String) ctxt.getAttribute("apiKey");

        boolean useheader = Boolean.FALSE != ctxt.getAttribute("useHeader");

        Map.Entry<List<SchemaField>, int[]> header;
        if (useheader) {
            header = readHeader((CsvParser) jp, project, collection);
        } else {
            List<SchemaField> vall = metastore.getCollection(project, collection);
            header = new AbstractMap.SimpleImmutableEntry<>(vall, IntStream.range(0, vall.size()).toArray());
        }

        List<SchemaField> fields = header.getKey();
        int[] indexes = header.getValue();
        List<FieldType> types = Arrays.stream(indexes)
                .mapToObj(i -> header.getKey().get(i).getType()).collect(Collectors.toList());

        Schema schema = convertAvroSchema(fields);
        GenericData.Record record = new GenericData.Record(schema);
        int idx = 0;

        List<Event> list = new ArrayList<>();
        while (true) {
            JsonToken t = jp.nextToken();

            if (t == null) {
                break;
            }

            switch (t.id()) {
                case JsonTokenId.ID_START_ARRAY:
                    idx = 0;
                    record = new GenericData.Record(schema);
                    list.add(new Event(project, collection, null, fields, record));
                    break;
                case JsonTokenId.ID_END_ARRAY:
                    continue;
                default:
                    if (idx >= indexes.length) {
                        throw new RakamException(String.format("Table has %d columns but csv file has more than %d columns", indexes.length, indexes.length), HttpResponseStatus.BAD_REQUEST);
                    }
                    record.put(indexes[idx], getValue(types.get(idx), jp));
                    idx += 1;
                    break;
            }
        }

        return new EventList(Event.EventContext.apiKey(apiKey), project, list);
    }

    public Map.Entry<List<SchemaField>, int[]> readHeader(CsvParser jp, String project, String collection)
            throws IOException {
        List<SchemaField> fields = metastore.getCollection(project, collection);
        if (fields.isEmpty()) {
            fields = ImmutableList.copyOf(constantFields);
        }
        List<String> columns = new ArrayList<>();

        Set<SchemaField> newFields = new HashSet<>();
        while (jp.nextToken() == VALUE_STRING) {
            String name = ValidationUtil.stripName(jp.getValueAsString(), "header name");

            Optional<SchemaField> existingField = fields.stream()
                    .filter(f -> f.getName().equals(name)).findAny();

            if (!existingField.isPresent()) {
                FieldType type = STRING;
                if (name.equals(projectConfig)) {
                    type = configManager.setConfigOnce(project, USER_TYPE.name(), STRING);
                }
                SchemaField field = dependentFields.values().stream()
                        .flatMap(e -> e.stream())
                        .filter(e -> e.getName().equals(name))
                        .findAny().orElse(new SchemaField(name, type));
                newFields.add(field);
            }

            columns.add(name);
        }

        if (!newFields.isEmpty()) {
            fields = metastore.getOrCreateCollectionFields(project, collection,
                    schemaChecker.checkNewFields(collection, newFields));
        }

        final List<SchemaField> finalFields = fields;
        int[] indexes = columns.stream().mapToInt(colName -> range(0, finalFields.size())
                .filter(i -> finalFields.get(i).getName().equals(colName)).findAny().getAsInt())
                .toArray();

        return new AbstractMap.SimpleImmutableEntry<>(fields, indexes);
    }

    public Object getValue(FieldType type, JsonParser jp)
            throws IOException {
        if (type == null) {
            return getValueOfMagicField(jp);
        }

        switch (type) {
            case STRING:
                String valueAsString = jp.getValueAsString();
                if (valueAsString.length() > 100) {
                    valueAsString = valueAsString.substring(0, 100);
                }
                return valueAsString;
            case BOOLEAN:
                return jp.getValueAsBoolean();
            case LONG:
                return jp.getValueAsLong();
            case INTEGER:
                return jp.getValueAsInt();
            case DECIMAL:
                return jp.getValueAsDouble();
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
                } catch (Exception e) {
                    return null;
                }
            case DATE:
                try {
                    return DateTimeUtils.parseDate(jp.getValueAsString());
                } catch (Exception e) {
                    return null;
                }
            default:
                if (type.isMap()) {
                    return getMap(type.getMapValueType(), jp.getValueAsString());
                }
                if (type.isArray()) {
                    return getArray(type.getArrayElementType(), jp.getValueAsString());
                }
                throw new JsonMappingException(format("type is not supported."));
        }
    }

    private GenericData.Array getArray(FieldType arrayElementType, String valueAsString)
            throws IOException {
        JsonParser parser = jsonFactory.createParser(valueAsString);

        List<Object> objects = new ArrayList<>();

        JsonToken t = parser.getCurrentToken();
        if (t != JsonToken.START_ARRAY) {
            return null;
        } else {
            t = parser.nextToken();
        }

        for (; t != JsonToken.END_ARRAY; t = parser.nextToken()) {
            if (!t.isScalarValue()) {
                throw new JsonMappingException(String.format("Nested properties are not supported. ('%s' field)", arrayElementType.name()));
            }
            objects.add(getValue(arrayElementType, parser));
        }

        return new GenericData.Array(generateAvroSchema(arrayElementType), objects);
    }

    private Map<String, Object> getMap(FieldType mapValueType, String valueAsString)
            throws IOException {
        Map<String, Object> map = new HashMap<>();
        JsonParser parser = jsonFactory.createParser(valueAsString);

        JsonToken t = parser.getCurrentToken();
        if (t != JsonToken.START_OBJECT) {
            return null;
        } else {
            t = parser.nextToken();
        }

        for (; t == JsonToken.FIELD_NAME; t = parser.nextToken()) {
            String key = parser.getCurrentName();

            if (!parser.nextToken().isScalarValue()) {
                throw new JsonMappingException(String.format("Nested properties are not supported. ('%s' field)", mapValueType.name()));
            }

            map.put(key, getValue(mapValueType, parser));
        }

        return map;
    }
}
