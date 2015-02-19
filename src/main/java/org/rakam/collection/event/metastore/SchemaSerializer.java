package org.rakam.collection.event.metastore;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/02/15 01:28.
 */
public class SchemaSerializer extends StdSerializer<Schema> {
    public SchemaSerializer() { super(Schema.class); }

    @Override
    public void serialize(Schema value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeRawValue(value.toString());
    }
}
