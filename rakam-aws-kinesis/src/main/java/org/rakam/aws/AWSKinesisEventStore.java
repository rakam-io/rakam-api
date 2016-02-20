package org.rakam.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.analysis.metadata.Metastore;
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
    private static final int BULK_THRESHOLD = 50000;
    private final S3BulkEventStore bulkClient;
    private final Metastore metastore;

    ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>() {
        @Override
        protected KByteArrayOutputStream initialValue() {
            return new KByteArrayOutputStream(500000);
        }
    };

    @Inject
    public AWSKinesisEventStore(AWSConfig config,
                                Metastore metastore,
                                FieldDependency fieldDependency) {
        kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        if(config.getKinesisEndpoint() != null) {
            kinesis.setEndpoint(config.getKinesisEndpoint());
        }

        this.sourceFields = fieldDependency.dependentFields.keySet();
        this.config = config;
        this.metastore = metastore;
        this.bulkClient = new S3BulkEventStore(metastore, config);
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
                System.out.println("error "+putRecordsResult.getFailedRecordCount()+": "+putRecordsResult.getRecords().get(0).getErrorMessage());
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
        if(events.size() > BULK_THRESHOLD) {
            bulkClient.upload(events.get(0).project(), events);
        } else {

            if (events.size() > BATCH_SIZE) {
                int cursor = 0;

                while (cursor < events.size()) {
                    int loopSize = Math.min(BATCH_SIZE, events.size() - cursor);

                    storeBatchInline(events, cursor, loopSize);
                    cursor += loopSize;
                }
            } else {
                storeBatchInline(events, 0, events.size());
            }
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
        DatumWriter writer = new FilteredRecordWriter(event.properties().getSchema(), GenericData.get());
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

}