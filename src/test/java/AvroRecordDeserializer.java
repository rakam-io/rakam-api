import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.avro.generic.GenericData;
import org.rakam.collection.event.metastore.EventSchemaMetastore;

import java.io.IOException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 20:35.
 */


public class AvroRecordDeserializer extends JsonDeserializer<GenericData.Record> {

    private final EventSchemaMetastore registry;

    public AvroRecordDeserializer(EventSchemaMetastore registry) {
        this.registry = registry;
    }

    @Override
    public GenericData.Record deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        GenericData.Record objectObjectHashMap = new GenericData.Record(null);
        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            t = jp.nextToken();
            try {
                Object value;
                switch (t) {
                    case VALUE_NULL:
                        value = null;
                        break;
                    case VALUE_STRING:
                        value = null;
                        break;
                    case VALUE_FALSE:
                        value = false;
                        break;
                    case VALUE_NUMBER_FLOAT:
                        value = false;
                        break;
                    case VALUE_TRUE:
                        value = true;
                        break;
                    case VALUE_EMBEDDED_OBJECT:
                        value = true;
                        break;
                    case VALUE_NUMBER_INT:
                        value = true;
                        break;
                    default:
                        continue;
                }

                objectObjectHashMap.put(fieldName, value);

            } catch (Exception e) {
                System.out.println(1);
            }
        }
        return null;
    }
}
