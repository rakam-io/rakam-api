package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;

import java.io.IOException;

public class FilteredRecordWriter extends GenericDatumWriter {
    private final GenericData data;

    public FilteredRecordWriter(Schema root, GenericData data) {
        super(root, data);
        this.data = data;
    }

    @Override
    public void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
        Object state = data.getRecordState(datum, schema);
        for (Schema.Field f : schema.getFields()) {
            if (f.schema().getType() != Schema.Type.NULL) {
                writeField(datum, f, out, state);
            }
        }
    }
}
