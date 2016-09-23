package org.rakam.collection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.util.DateTimeUtils;

import javax.inject.Inject;

import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.rakam.analysis.InternalConfig.USER_TYPE;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.collection.JsonEventDeserializer.getValueOfMagicField;
import static org.rakam.util.AvroUtil.convertAvroSchema;

public class CsvEventDeserializer
        extends JsonDeserializer<EventList>
{

    private final Metastore metastore;
    private final Set<SchemaField> constantFields;
    private final ConfigManager configManager;

    @Inject
    public CsvEventDeserializer(Metastore metastore, ConfigManager configManager,
            FieldDependency fieldDependency)
    {
        this.metastore = metastore;

        this.configManager = configManager;
        this.constantFields = fieldDependency.constantFields;
    }

    @Override
    public EventList deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException
    {
        String project = (String) ctxt.getAttribute("project");
        String collection = (String) ctxt.getAttribute("collection");
        String apiKey = (String) ctxt.getAttribute("apiKey");

        boolean useheader = (boolean) ctxt.getAttribute("useHeader");

        Map.Entry<List<SchemaField>, int[]> header;
        if(useheader) {
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
                    record.put(indexes[idx], getValue(types.get(idx), jp));
                    idx += 1;
                    break;
            }
        }

        return new EventList(Event.EventContext.apiKey(apiKey), project, list);
    }

    public Map.Entry<List<SchemaField>, int[]> readHeader(CsvParser jp, String project, String collection)
            throws IOException
    {
        List<SchemaField> fields = metastore.getCollection(project, collection);
        if (fields.isEmpty()) {
            fields = ImmutableList.copyOf(constantFields);
        }
        List<String> columns = new ArrayList<>();

        Set<SchemaField> newFields = new HashSet<>();
        while (jp.nextToken() == VALUE_STRING) {
            String name = SchemaField.stripName(jp.getValueAsString());

            Optional<SchemaField> existingField = fields.stream()
                    .filter(f -> f.getName().equals(name)).findAny();

            if (!existingField.isPresent()) {
                FieldType type = STRING;
                if (name.equals("_user")) {
                    type = configManager.setConfigOnce(project, USER_TYPE.name(), STRING);
                }
                newFields.add(new SchemaField(name, type));
            }

            columns.add(name);
        }

        if (!newFields.isEmpty()) {
            fields = metastore.getOrCreateCollectionFieldList(project, collection, newFields);
        }

        final List<SchemaField> finalFields = fields;
        int[] indexes = columns.stream().mapToInt(colName -> range(0, finalFields.size())
                .filter(i -> finalFields.get(i).getName().equals(colName)).findAny().getAsInt())
                .toArray();

        return new AbstractMap.SimpleImmutableEntry<>(fields, indexes);
    }

    public Object getValue(FieldType type, JsonParser jp)
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
                if (type.isMap()) {
                    throw new UnsupportedOperationException("map type is not supported");
                }
                if (type.isArray()) {
                    throw new UnsupportedOperationException("array type is not supported");
                }
                throw new JsonMappingException(format("type is not supported."));
        }
    }
}
