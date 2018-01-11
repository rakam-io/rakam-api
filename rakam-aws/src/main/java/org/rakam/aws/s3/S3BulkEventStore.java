package org.rakam.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.base.Throwables;
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
    private final AmazonS3Client s3Client;
    private final AWSConfig config;
    private final int conditionalMagicFieldsSize;
    private final AmazonCloudWatchAsyncClient cloudWatchClient;
    private final AmazonKinesisClient kinesis;

    public S3BulkEventStore(Metastore metastore, AWSConfig config, FieldDependencyBuilder.FieldDependency fieldDependency) {
        this.metastore = metastore;
        this.config = config;
        this.s3Client = new AmazonS3Client(config.getCredentials());
        s3Client.setRegion(config.getAWSRegion());
        if (config.getS3Endpoint() != null) {
            s3Client.setEndpoint(config.getS3Endpoint());
        }

        kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        if (config.getKinesisEndpoint() != null) {
            kinesis.setEndpoint(config.getKinesisEndpoint());
        }

        cloudWatchClient = new AmazonCloudWatchAsyncClient(config.getCredentials());
        cloudWatchClient.setRegion(config.getAWSRegion());

        this.conditionalMagicFieldsSize = fieldDependency.dependentFields.size();
    }

    public void upload(String project, List<Event> events, int tryCount) {
        GenericData data = GenericData.get();

        DynamicSliceOutput buffer = new DynamicSliceOutput(events.size() * 30);

        Map<String, List<Event>> map = new HashMap<>();
        events.forEach(event -> map.computeIfAbsent(event.collection(),
                (col) -> new ArrayList<>()).add(event));

        BinaryEncoder encoder = null;

        String batchId = UUID.randomUUID().toString();

        List<String> uploadedFiles = new ArrayList<>();
        try {
            for (Map.Entry<String, List<Event>> entry : map.entrySet()) {
                buffer.reset();

                List<SchemaField> collection = metastore.getCollection(project, entry.getKey());

                Schema avroSchema = convertAvroSchema(collection);
                DatumWriter writer = new FilteredRecordWriter(avroSchema, data);
                encoder = EncoderFactory.get().directBinaryEncoder(buffer, encoder);

                encoder.writeInt(collection.size());
                for (SchemaField schemaField : collection) {
                    encoder.writeString(schemaField.getName());
                }

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

                ObjectMetadata objectMetadata = new ObjectMetadata();
                int bulkSize = buffer.size();
                objectMetadata.setContentLength(bulkSize);
                String key = events.get(0).project() + "/" + entry.getKey() + "/" + batchId;
                PutObjectRequest putObjectRequest = new PutObjectRequest(config.getEventStoreBulkS3Bucket(),
                        key,
                        new SafeSliceInputStream(new BasicSliceInput(buffer.slice())), objectMetadata);
                putObjectRequest.getRequestClientOptions().setReadLimit(bulkSize);

                s3Client.putObject(putObjectRequest);

                ByteBuffer allocate = ByteBuffer.allocate(key.length() + 1 + 8);
                allocate.put((byte) 1);
                allocate.putLong(bulkSize);
                allocate.put(key.getBytes(StandardCharsets.UTF_8));
                allocate.clear();

                putMetadataToKinesis(allocate, events.get(0).project(), entry.getKey(), 3);
                uploadedFiles.add(key);
            }

            LOGGER.debug("Stored batch file '%s', %d events in %d collection.", batchId, events.size(), map.size());

            cloudWatchClient.putMetricDataAsync(new PutMetricDataRequest()
                    .withNamespace("rakam-middleware-collection")
                    .withMetricData(new MetricDatum()
                            .withMetricName("bulk")
                            .withValue(((Number) events.size()).doubleValue())
                            .withDimensions(new Dimension().withName("project").withValue(project))));
        } catch (IOException | AmazonClientException e) {
            for (String uploadedFile : uploadedFiles) {
                s3Client.deleteObject(config.getEventStoreBulkS3Bucket(), uploadedFile);
            }
            if (tryCount <= 0) {
                throw Throwables.propagate(e);
            }

            upload(project, events, tryCount - 1);
        }
    }

    private void putMetadataToKinesis(ByteBuffer allocate, String project, String collection, int tryCount) {
        try {
            kinesis.putRecord(config.getEventStoreStreamName(), allocate,
                    project + "|" + collection);
        } catch (Exception e) {
            if (tryCount == 0) {
                throw e;
            }

            putMetadataToKinesis(allocate, project, collection, tryCount - 1);
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
