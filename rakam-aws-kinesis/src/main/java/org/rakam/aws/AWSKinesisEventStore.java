package org.rakam.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.RecordGenericRecordWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.collection.Event;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;
import org.rakam.util.KByteArrayOutputStream;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.rakam.aws.KinesisUtils.createAndWaitForStreamToBecomeAvailable;

public class AWSKinesisEventStore implements EventStore {
    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private final Set<String> sourceFields;
    private static final int BATCH_SIZE = 500;

    ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>() {
        @Override
        protected KByteArrayOutputStream initialValue() {
            return new KByteArrayOutputStream(500000);
        }
    };

    @Inject
    public AWSKinesisEventStore(AWSConfig config, FieldDependencyBuilder.FieldDependency fieldDependency) {
        this.kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        this.sourceFields = fieldDependency.dependentFields.keySet();
        this.config = config;
    }

    public void storeBatchInline(List<Event> events, int offset, int limit) {
        PutRecordsRequestEntry[] records = new PutRecordsRequestEntry[limit];

        for (int i = 0; i < limit; i++) {
            Event event = events.get(offset + i);
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry()
                    .withData(getBuffer(event))
                    .withPartitionKey(event.project() + "|" + event.collection());
            records[i] = putRecordsRequestEntry;
        }

        try {
            PutRecordsResult putRecordsResult = kinesis.putRecords(new PutRecordsRequest()
                    .withRecords(records)
                    .withStreamName(config.getEventStoreStreamName()));
            if (putRecordsResult.getFailedRecordCount() > 0) {
                for (PutRecordsResultEntry resultEntry : putRecordsResult.getRecords()) {
                    resultEntry.getErrorMessage();
                }
            }
        } catch (ResourceNotFoundException e) {
            try {
                createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
            } catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    @Override
    public void storeBatch(List<Event> events) {
        if(events.size() > BATCH_SIZE) {
            int cursor = 0;

            while(cursor < events.size()) {
                int loopSize = Math.min(BATCH_SIZE, events.size() - cursor);

                storeBatchInline(events, cursor, loopSize);
                cursor += loopSize;
            }
        } else {
            storeBatchInline(events, 0, events.size());
        }
    }

    @Override
    public void store(Event event) {
        try {
            kinesis.putRecord(config.getEventStoreStreamName(), getBuffer(event),
                    event.project() + "|" + event.collection());
        } catch (ResourceNotFoundException e) {
            try {
                createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
            } catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    private ByteBuffer getBuffer(Event event) {
        DatumWriter writer = new RecordGenericRecordWriter(event.properties().getSchema(), GenericData.get(), sourceFields);
        KByteArrayOutputStream out = buffer.get();

        int startPosition = out.position();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

        try {
            writer.write(event.properties(), encoder);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't serialize event", e);
        }

        int endPosition = out.position();
        // TODO: find a way to make it zero-copy

        if (out.remaining() < 1000) {
            out.position(0);
        }

        return out.getBuffer(startPosition, endPosition - startPosition);
    }

//    public class S3BulkEventStore {
//        public void upload(List<Event> events) {
//
//            Map<String, OutputStream> map = new HashMap<>();
//            for (Event event : events) {
//                OutputStream buffer = map.get(event.collection());
//                if (buffer == null) {
//                    buffer = new DynamicSliceOutput(events.size() * 100);
//                    map.put(event.collection(), buffer);
//                }
//                DatumWriter writer = new RecordGenericRecordWriter(event.properties().getSchema(), GenericData.get(), sourceFields);
//                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
//                try {
//                    writer.write(event.properties(), encoder);
//                } catch (Exception e) {
//                    throw new RuntimeException("Couldn't serialize event", e);
//                }
//            }
//        }
//    }
}