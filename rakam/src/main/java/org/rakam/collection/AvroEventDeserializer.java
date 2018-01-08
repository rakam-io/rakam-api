package org.rakam.collection;

import io.airlift.slice.SliceInput;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.util.AvroUtil;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroEventDeserializer {

    private final Metastore metastore;

    @Inject
    public AvroEventDeserializer(Metastore metastore) {
        this.metastore = metastore;
    }

    public EventList deserialize(String project, String collection, SliceInput slice) throws IOException {
        String json = slice.readSlice(slice.readInt()).toStringUtf8();
        Schema schema = new Schema.Parser().parse(json);
        int records = slice.readInt();

        BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(slice, null);

        List<SchemaField> fields = metastore.getCollection(project, collection);
        Schema avroSchema = AvroUtil.convertAvroSchema(fields);

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader(schema, avroSchema);

        List<Event> list = new ArrayList<>(records);
        for (int i = 0; i < records; i++) {
            GenericRecord record = reader.read(null, binaryDecoder);
            list.add(new Event(project, collection, null, fields, record));
        }

        return new EventList(Event.EventContext.empty(), project, list);
    }
}
