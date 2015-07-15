package org.rakam.analysis;


import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rakam.collection.Event;
import org.rakam.util.Tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This implementaion of IEmitter inserts records into Amazon S3 and emits filenames into a separate
 * Amazon Kinesis stream. The separate Amazon Kinesis stream is to be used by another Amazon Kinesis enabled application
 * that utilizes RedshiftManifestEmitters to insert the records into Amazon Redshift via a manifest copy.
 * This class requires the configuration of an Amazon S3 bucket and endpoint, as well as Amazon Kinesis endpoint
 * and output stream.
 * <p>
 * When the buffer is full, this Emitter:
 * <ol>
 * <li>Puts all records into a single file in S3</li>
 * <li>Puts the single file name into the manifest stream</li>
 * </ol>
 * <p>
 * NOTE: the Amazon S3 bucket and Amazon Redshift cluster must be in the same region.
 */
public class S3ManifestEmitter implements IEmitter<Event> {
    private static final Log LOG = LogFactory.getLog(S3ManifestEmitter.class);
    private final AmazonKinesisClient kinesisClient;
    private final String manifestStream;
    private final String inputStream;
    private final AmazonS3Client s3client;
    private final String s3Bucket;

    public S3ManifestEmitter(KinesisConnectorConfiguration configuration) {
        manifestStream = configuration.KINESIS_OUTPUT_STREAM;
        inputStream = configuration.KINESIS_INPUT_STREAM;
        s3Bucket = configuration.S3_BUCKET;
        s3client = new AmazonS3Client(configuration.AWS_CREDENTIALS_PROVIDER);

        kinesisClient = new AmazonKinesisClient(configuration.AWS_CREDENTIALS_PROVIDER);
        if (configuration.KINESIS_ENDPOINT != null) {
            kinesisClient.setEndpoint(configuration.KINESIS_ENDPOINT);
        }
    }

    @Override
    public List<Event> emit(final UnmodifiableBuffer<Event> buffer) throws IOException {
        List<Event> records = buffer.getRecords();
        Map<Tuple<String, String>, ByteArrayOutputStream> map = new HashMap<>();

        for (Event record : records) {
            ByteArrayOutputStream baos = map.get(new Tuple<>(record.project(), record.collection()));
            if(baos == null) {
                baos = new ByteArrayOutputStream();
                map.put(new Tuple<>(record.project(), record.collection()), baos);
            }
            try {
                baos.write(toDelimitedString(record).getBytes());
            } catch (Exception e) {
                LOG.error("Error writing record to output stream. Failing this emit attempt. Record: " + record, e);
                return buffer.getRecords();
            }
        }

        for (Map.Entry<Tuple<String, String>, ByteArrayOutputStream> entry : map.entrySet()) {
            // Get the Amazon S3 filename
            String s3FileName = getS3FileName(entry.getKey().v1(), entry.getKey().v2(), buffer.getFirstSequenceNumber(), buffer.getLastSequenceNumber());
            String s3URI = getS3URI(s3FileName);
            try {
                ByteArrayInputStream object = new ByteArrayInputStream(entry.getValue().toByteArray());
                LOG.debug("Starting upload of file " + s3URI + " to Amazon S3 containing " + records.size() + " records.");
                s3client.putObject(s3Bucket, s3FileName, object, null);
                LOG.info("Successfully emitted " + buffer.getRecords().size() + " records to Amazon S3 in " + s3URI);
            } catch (Exception e) {
                LOG.error("Caught exception when uploading file " + s3URI + "to Amazon S3. Failing this emit attempt.", e);
                return buffer.getRecords();
            }

            ByteBuffer data = ByteBuffer.wrap(s3FileName.getBytes());
            // Put the list of file names to the manifest Amazon Kinesis stream
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setData(data);
            putRecordRequest.setStreamName(manifestStream);
            // Use constant partition key to ensure file order
            putRecordRequest.setPartitionKey(entry.getKey().v1()+ "_" +entry.getKey().v2());
            try {
                kinesisClient.putRecord(putRecordRequest);
                LOG.info("S3ManifestEmitter emitted record downstream: " + s3FileName);
            } catch (Exception e) {
                LOG.error(e);
                return buffer.getRecords();
            }
        }

        return Collections.emptyList();
    }

    protected String getS3FileName(String project, String collection, String firstSeq, String lastSeq) {
        return project+"/"+collection+"/"+firstSeq + "-" + lastSeq;
    }

    protected String getS3URI(String s3FileName) {
        return "s3://" + s3Bucket + "/" + s3FileName;
    }

    public String toDelimitedString(Event record) {
        GenericRecord properties = record.properties();
        int size = properties.getSchema().getFields().size() - 1;
        StringBuilder b = new StringBuilder();

        for (int i = 0; i < size; i++) {
            Object obj = properties.get(i);
            if(obj != null) {
                b.append(obj);
            }
            b.append("|");
        }
        b.append(properties.get(size))
                .append("\n");

        return b.toString();
    }

    @Override
    public void fail(List<Event> records) {
        for (Event record : records) {
            LOG.error("Record failed: " + record);
        }
    }

    @Override
    public void shutdown() {
        s3client.shutdown();
    }
}