package org.rakam.collection.event;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.ResolvingDecoder;

import java.io.IOException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/02/15 15:20.
 */
public class CustomGenericDatumReader<D> extends GenericDatumReader<D> {

    private BinaryDecoder in;
    private Schema schema;

    public CustomGenericDatumReader(Schema schema) {
        super(schema);
        this.schema = schema;
    }

    @SuppressWarnings("unchecked")
    public D read(D reuse, BinaryDecoder in) throws IOException {
        this.in = in;

        ResolvingDecoder resolver = getResolver(schema, schema);
        resolver.configure(in);
        D result = (D) read(reuse, schema, resolver);
//        resolver.drain();
        return result;
    }

    protected void readField(Object r, Schema.Field f, Object oldDatum,
                             ResolvingDecoder in) throws IOException {
        getData().setField(r, f.name(), f.pos(), read(oldDatum, f.schema(), in));
    }

    /** Called to read a record instance. May be overridden for alternate record
     * representations.*/
    protected Object readRecord(Object old, Schema expected,
                                ResolvingDecoder in) throws IOException {
        GenericData data = super.getData();
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
            readField(r, f, oldDatum, in);
        }

        return r;
    }
}
