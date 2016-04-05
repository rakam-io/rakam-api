import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.primitives.Ints;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.EventCollectionHttpService.EventList;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static java.lang.String.format;
import static org.rakam.collection.EventDeserializer.getValueOfMagicField;
import static org.rakam.util.AvroUtil.convertAvroSchema;

public class TestCSVParser {
    @Test
    public void testName() throws Exception {
        ObjectMapper mapper = new CsvMapper();
        mapper.registerModule(new SimpleModule().addDeserializer(EventList.class, new MyJsonDeserializer(null)));
        String csv = "Transaction_date,Product,Price,Payment_Type,Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude\n" +
                "1/2/09 6:17,Product1,1200,Mastercard,carolina,Basildon,England,United Kingdom,1/2/09 6:00,1/2/09 6:08,51.5,-1.1166667\n" +
                "1/2/09 4:53,Product1,1200,Visa,Betina,Parkville                   ,MO,United States,1/2/09 4:42,1/2/09 7:49,39.195,-94.68194\n" +
                "1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea,Astoria                     ,OR,United States,1/1/09 16:21,1/3/09 12:32,46.18806,-123.83\n" +
                "1/3/09 14:44,Product1,1200,Visa,Gouya,Echuca,Victoria,Australia,9/25/05 21:13,1/3/09 14:22,-36.1333333,144.75\n";

        EventList o = mapper.reader(EventList.class).with(ContextAttributes.getEmpty()
                        .withSharedAttribute("project", "project")
                        .withSharedAttribute("collection", "collection")
        ).readValue(csv);

        System.out.println(o);
    }

    public static class MyJsonDeserializer extends JsonDeserializer<EventList> {

        private final Metastore metastore;

        public MyJsonDeserializer(Metastore metastore) {
            this.metastore = metastore;
        }

        @Override
        public EventList deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            List<SchemaField> fields = readHeader((CsvParser) jp,
                    (String) ctxt.getAttribute("project"),
                    (String) ctxt.getAttribute("collection"));

            Schema schema = convertAvroSchema(fields);
            GenericData.Record record = new GenericData.Record(schema);;
            int idx = 0;

            while (true) {
                JsonToken t = jp.nextToken();

                if (t == null) {
                    throw ctxt.mappingException("Unexpected end-of-input when binding data");
                }

                switch (t.id()) {
                    case JsonTokenId.ID_START_ARRAY:
                        idx = 0;
                        record = new GenericData.Record(schema);
                        break;
                    case JsonTokenId.ID_END_ARRAY:
                        continue;
                    case JsonTokenId.ID_FIELD_NAME:
                        idx++;
                        break;
                    default:
                        record.put(idx, getValue(fields.get(idx).getType(), jp));
                        throw new UnsupportedOperationException();
                }

                break;
            }
            return null;
        }

        public List<SchemaField> readHeader(CsvParser jp, String project, String collection) throws IOException {
            CsvSchema.Builder builder = CsvSchema.builder();

            ArrayList<SchemaField> list = new ArrayList<>();

            String name;
            while (jp.nextToken() == FIELD_NAME) {
                jp.nextToken();

                name = jp.getValueAsString().trim();
                builder.addColumn(name);
                list.add(new SchemaField(name, FieldType.STRING));
            }

            jp.nextToken();
            jp.setSchema(builder.build());
            return list;
        }

        public Object getValue(FieldType type, JsonParser jp) throws IOException {
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
}
