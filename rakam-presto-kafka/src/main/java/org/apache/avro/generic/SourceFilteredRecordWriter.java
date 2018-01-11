package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;

import java.io.IOException;
import java.util.Set;

public class SourceFilteredRecordWriter extends GenericDatumWriter {
    private final GenericData data;
    private final Set<String> sourceFields;

    public SourceFilteredRecordWriter(Schema root, GenericData data, Set<String> sourceFields) {
        super(root, data);
        this.data = data;
        this.sourceFields = sourceFields;
    }

    @Override
    public void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
        Object state = data.getRecordState(datum, schema);
        for (Schema.Field f : schema.getFields()) {
            if (!sourceFields.contains(f.name())) {
                writeField(datum, f, out, state);
            }
        }
    }
}
