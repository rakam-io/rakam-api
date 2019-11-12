package org.rakam.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.*;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import org.apache.avro.Schema;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.AWSConfig;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.SchemaField;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.rakam.util.AvroUtil.convertAvroSchema;

public class S3BulkEventStore {
    private final static Logger LOGGER = Logger.get(S3BulkEventStore.class);
    private final Metastore metastore;
    private final AmazonS3 s3Client;
    private final AWSConfig config;
    private final int conditionalMagicFieldsSize;
    private final AmazonCloudWatchAsync cloudWatchClient;
    private final AmazonKinesis kinesis;

    public S3BulkEventStore(Metastore metastore, AWSConfig config, FieldDependencyBuilder.FieldDependency fieldDependency) {
        this.metastore = metastore;
        this.config = config;
        AmazonS3ClientBuilder builder = AmazonS3Client.builder().withCredentials(config.getCredentials());
        if(config.getRegion() != null) {
            builder.setRegion(config.getRegion());
        }
        if(config.getS3Endpoint() != null) {
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.getS3Endpoint(), null));
        }

        AmazonKinesisClientBuilder kinesisBuilder = AmazonKinesisAsyncClient.builder().withCredentials(config.getCredentials());
        if (config.getRegion() != null) {
            kinesisBuilder.setRegion(config.getRegion());
        }
        if (config.getKinesisEndpoint() != null) {
            kinesisBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.getRegion(), null));
        }

        kinesis = kinesisBuilder.build();
        s3Client = builder.build();

        AmazonCloudWatchAsyncClientBuilder cwBuilder = AmazonCloudWatchAsyncClient.asyncBuilder().withCredentials(config.getCredentials());
        if(config.getRegion() != null) {
            cwBuilder.setRegion(config.getRegion());
        }
        cloudWatchClient = cwBuilder.build();

        this.conditionalMagicFieldsSize = fieldDependency.dependentFields.size();
    }

    public void upload(String project, List<Event> events, int tryCount) {
        GenericData data = GenericData.get();

        DynamicSliceOutput buffer = new DynamicSliceOutput(events.size() * 30);

        Map<String, List<Event>> map = new HashMap<>();
        events.forEach(event -> map.computeIfAbsent(event.collection(),
                (col) -> new ArrayList<>()).add(event));

        String batchId = UUID.randomUUID().toString();
        String key = events.get(0).project() + "/" + batchId;

        try {
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
            encoder.writeString(project);

            for (Map.Entry<String, List<Event>> entry : map.entrySet()) {
                String collectionName = entry.getKey();
                List<SchemaField> collection = metastore.getCollection(project, collectionName);

                Schema avroSchema = convertAvroSchema(collection);
                DatumWriter writer = new FilteredRecordWriter(avroSchema, data);

                encoder.writeString(collectionName);

                encoder.writeInt(collection.size());
                encoder.writeInt(entry.getValue().size());

                int expectedSchemaSize = collection.size() + conditionalMagicFieldsSize;
                for (Event event : entry.getValue()) {
                    GenericRecord properties = event.properties();

                    List<Schema.Field> existingFields = properties.getSchema().getFields();
                    if (existingFields.size() != expectedSchemaSize) {
                        GenericData.Record record = new GenericData.Record(avroSchema);
                        for (int i = 0; i < existingFields.size(); i++) {
                            if (existingFields.get(i).schema().getType() != Schema.Type.NULL) {
                                record.put(i, properties.get(i));
                            }
                        }
                        properties = record;
                    }
                    writer.write(properties, encoder);
                }
            }

            ObjectMetadata objectMetadata = new ObjectMetadata();
            int bulkSize = buffer.size();
            objectMetadata.setContentLength(bulkSize);
            PutObjectRequest putObjectRequest = new PutObjectRequest(config.getEventStoreBulkS3Bucket(),
                    key,
                    new SafeSliceInputStream(new BasicSliceInput(buffer.slice())), objectMetadata);
            putObjectRequest.getRequestClientOptions().setReadLimit(bulkSize);

            s3Client.putObject(putObjectRequest);

            ByteBuffer allocate = ByteBuffer.allocate(key.length() + 1 + 8);
            allocate.put((byte) 3);
            allocate.putLong(bulkSize);
            allocate.put(key.getBytes(StandardCharsets.UTF_8));
            allocate.clear();

            putMetadataToKinesis(allocate, batchId, 3);

            LOGGER.debug("Stored batch file '%s', %d events in %d collection.", batchId, events.size(), map.size());

            cloudWatchClient.putMetricDataAsync(new PutMetricDataRequest()
                    .withNamespace("rakam-middleware-collection")
                    .withMetricData(new MetricDatum()
                            .withMetricName("bulk")
                            .withValue(((Number) events.size()).doubleValue())
                            .withDimensions(new Dimension().withName("project").withValue(project))));
        } catch (IOException | AmazonClientException e) {
            s3Client.deleteObject(config.getEventStoreBulkS3Bucket(), key);

            if (tryCount <= 0) {
                throw new RuntimeException(e);
            }

            upload(project, events, tryCount - 1);
        }
    }

    private void putMetadataToKinesis(ByteBuffer allocate, String id, int tryCount) {
        try {
            kinesis.putRecord(config.getEventStoreStreamName(), allocate, id);
        } catch (Exception e) {
            if (tryCount == 0) {
                throw e;
            }

            putMetadataToKinesis(allocate, id, tryCount - 1);
        }
    }

    private class SafeSliceInputStream
            extends InputStream {
        private final BasicSliceInput sliceInput;

        public SafeSliceInputStream(BasicSliceInput sliceInput) {
            this.sliceInput = sliceInput;
        }

        @Override
        public int read()
                throws IOException {
            return sliceInput.read();
        }

        @Override
        public int read(byte[] b)
                throws IOException {
            return sliceInput.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException {
            return sliceInput.read(b, off, len);
        }

        @Override
        public long skip(long n)
                throws IOException {
            return sliceInput.skip(n);
        }

        @Override
        public int available()
                throws IOException {
            return sliceInput.available();
        }

        @Override
        public void close()
                throws IOException {
            sliceInput.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            throw new RuntimeException("mark/reset not supported");
        }

        @Override
        public synchronized void reset()
                throws IOException {
            throw new IOException("mark/reset not supported");
        }

        @Override
        public boolean markSupported() {
            return false;
        }
    }
}
