package org.rakam.collection;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slices;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.EventCollectionHttpService.EventList;
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

    public EventList deserialize(String project, String collection, String apiKey, String buff) throws IOException {
        BasicSliceInput slice = Slices.utf8Slice(buff).getInput();
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
            list.add(new Event(project, collection, null, record));
        }

        Event.EventContext api = new Event.EventContext(apiKey, null, null, null);
        return new EventList(api, project, list);
    }
}
