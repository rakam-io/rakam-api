package org.rakam.analysis;


import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

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
public class S3ManifestEmitter extends S3Emitter {
    private static final Log LOG = LogFactory.getLog(S3ManifestEmitter.class);
    private final AmazonKinesisClient kinesisClient;
    private final String manifestStream;
    private final String inputStream;

    public S3ManifestEmitter(KinesisConnectorConfiguration configuration) {
        super(configuration);
        manifestStream = configuration.KINESIS_OUTPUT_STREAM;
        inputStream = configuration.KINESIS_INPUT_STREAM;
        kinesisClient = new AmazonKinesisClient(configuration.AWS_CREDENTIALS_PROVIDER);
        if (configuration.KINESIS_ENDPOINT != null) {
            kinesisClient.setEndpoint(configuration.KINESIS_ENDPOINT);
        }
    }

    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        // Store the contents of buffer.getRecords because superclass will
        // clear the buffer on success
        List<byte[]> failed = super.emit(buffer);
        // calls S3Emitter to write objects to Amazon S3
        if (!failed.isEmpty()) {
            return buffer.getRecords();
        }
        String s3File = getS3FileName(buffer.getFirstSequenceNumber(), buffer.getLastSequenceNumber());
        // wrap the name of the Amazon S3 file as the record data
        ByteBuffer data = ByteBuffer.wrap(s3File.getBytes());
        // Put the list of file names to the manifest Amazon Kinesis stream
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setData(data);
        putRecordRequest.setStreamName(manifestStream);
        // Use constant partition key to ensure file order
        putRecordRequest.setPartitionKey(inputStream);
        try {
            kinesisClient.putRecord(putRecordRequest);
            LOG.info("S3ManifestEmitter emitted record downstream: " + s3File);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error(e);
            return buffer.getRecords();
        }
    }

    @Override
    public void fail(List<byte[]> records) {
        super.fail(records);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        kinesisClient.shutdown();
    }

}