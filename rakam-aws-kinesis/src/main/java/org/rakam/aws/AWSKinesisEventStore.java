package org.rakam.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.rakam.aws.KinesisUtils.createAndWaitForStreamToBecomeAvailable;

public class AWSKinesisEventStore implements EventStore {
    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private final Set<String> sourceFields;
    private static final int BATCH_SIZE = 500;
    private static final int BULK_THRESHOLD = 5000;
    private final S3BulkEventStore bulkClient;

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
        this.bulkClient = new S3BulkEventStore();
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
            bulkClient.upload(events);
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
        System.out.println(events.size()+" committed");
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

    public class S3BulkEventStore {
        private final AmazonS3Client s3Client;

        public S3BulkEventStore() {
            this.s3Client = new AmazonS3Client(config.getCredentials());
            s3Client.setRegion(config.getAWSRegion());
        }

        public void upload(List<Event> events) {
            Map<String, DynamicSliceOutput> map = new HashMap<>();
            for (Event event : events) {
                DynamicSliceOutput buffer = map.get(event.collection());
                if (buffer == null) {
                    buffer = new DynamicSliceOutput(events.size() * 100);
                    map.put(event.collection(), buffer);
                }
                DatumWriter writer = new RecordGenericRecordWriter(event.properties().getSchema(), GenericData.get(), sourceFields);
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
                try {
                    writer.write(event.properties(), encoder);
                } catch (Exception e) {
                    throw new RuntimeException("Couldn't serialize event", e);
                }
            }

            String batchId = UUID.randomUUID().toString();

            for (Map.Entry<String, DynamicSliceOutput> entry : map.entrySet()) {
                int length = entry.getValue().slice().length();
                BasicSliceInput sliceInput = new BasicSliceInput(entry.getValue().slice());
                InputStream input = new SafeSliceInputStream(sliceInput);

                ObjectMetadata objectMetadata = new ObjectMetadata();
                objectMetadata.setContentLength(length);
                String key = events.get(0).project() + "/" + entry.getKey() + "/" + batchId;
                s3Client.putObject(config.getEventStoreBulkS3Bucket(), key, input, objectMetadata);
            }
        }

        private class SafeSliceInputStream extends InputStream {
            private final BasicSliceInput sliceInput;

            public SafeSliceInputStream(BasicSliceInput sliceInput) {
                this.sliceInput = sliceInput;
            }

            @Override
            public int read() throws IOException {
                return sliceInput.read();
            }

            @Override
            public int read(byte[] b) throws IOException {
                return sliceInput.read(b);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return sliceInput.read(b, off, len);
            }

            @Override
            public long skip(long n) throws IOException {
                return sliceInput.skip(n);
            }

            @Override
            public int available() throws IOException {
                return sliceInput.available();
            }

            @Override
            public void close() throws IOException {
                sliceInput.close();
            }

            @Override
            public synchronized void mark(int readlimit) {
                throw new RuntimeException("mark/reset not supported");
            }

            @Override
            public synchronized void reset() throws IOException {
                throw new IOException("mark/reset not supported");
            }

            @Override
            public boolean markSupported() {
                return false;
            }
        }
    }
}