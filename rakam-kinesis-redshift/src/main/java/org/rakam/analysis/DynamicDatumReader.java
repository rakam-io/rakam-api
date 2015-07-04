package org.rakam.analysis;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.ResolvingDecoder;

import java.io.IOException;

public class DynamicDatumReader<D> extends GenericDatumReader<D> {
    BinaryDecoder in;

    public DynamicDatumReader(Schema schema) {
        super(schema);
    }

    protected Object readRecord(Object old, Schema expected,
                                ResolvingDecoder in) throws IOException {
        GenericData data = getData();
        Object r = data.newRecord(old, expected);

        for (Schema.Field f : in.readFieldOrder()) {
            if(this.in.isEnd())
                break;
            int pos = f.pos();
            String name = f.name();
            Object oldDatum = null;
            if (old!=null) {
                oldDatum = data.getField(r, name, pos);
            }
            readField(r, f, oldDatum, in, null);
        }

        return r;
    }

    @SuppressWarnings("unchecked")
    public D read(D reuse, BinaryDecoder in) throws IOException {
        this.in = in;
        ResolvingDecoder resolver = getResolver(getSchema(), getSchema());
        resolver.configure(in);
        D result = (D) read(reuse, getSchema(), resolver);
//        resolver.drain();
        return result;
    }
}
