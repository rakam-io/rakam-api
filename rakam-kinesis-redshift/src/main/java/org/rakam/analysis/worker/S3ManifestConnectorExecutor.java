/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.rakam.analysis.worker;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import io.airlift.units.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rakam.analysis.AWSConfig;
import org.rakam.analysis.util.aws.S3Utils;
import org.rakam.collection.event.metastore.Metastore;

import java.rmi.dgc.VMID;
import java.util.Properties;

/**
 * This class defines the execution of a Amazon Kinesis Connector.
 * 
 */
public class S3ManifestConnectorExecutor implements Runnable {
    private static final Log LOG = LogFactory.getLog(S3ManifestConnectorExecutor.class);

    protected final KinesisConnectorConfiguration config;
    private final Properties properties;
    private final AWSConfig AWSConfig;
    private final Metastore metastore;
    private final String s3Bucket;
    private final Worker worker;

    public S3ManifestConnectorExecutor(AWSConfig config, Metastore metastore, String kinesisStream, String s3Bucket) {
        this.AWSConfig = config;
        this.metastore = metastore;
        this.s3Bucket = s3Bucket;

        properties = new Properties();
        properties.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, "rakam-kinesis-consumer-"+kinesisStream);
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, Integer.toString(1024 * 50));
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_OUTPUT_STREAM, config.getManifestStreamName());
        properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, kinesisStream);
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, Integer.toString(100));
        properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, Long.toString(Duration.valueOf("1m").toMillis()));
        properties.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, s3Bucket);

        this.config = new KinesisConnectorConfiguration(properties, AWSConfig.getCredentials());
        setupAWSResources();

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration("rakam-kinesis-consumer-"+kinesisStream,
                        kinesisStream,
                        AWSConfig.getCredentials(),
                        new VMID().toString())
                        .withUserAgent("rakam-kinesis-consumer-" + kinesisStream);


        worker = new Worker(getKinesisConnectorRecordProcessorFactory(), kinesisClientLibConfiguration);
        LOG.info(getClass().getSimpleName() + " worker created");
    }

    @Override
    public void run() {
        if (worker != null) {
            // Start Amazon Kinesis Client Library worker to process records
            LOG.info("Starting worker in " + getClass().getSimpleName());
            try {
                worker.run();
            } catch (Throwable t) {
                LOG.error(t);
                throw t;
            } finally {
                LOG.error("Worker " + getClass().getSimpleName() + " is not running.");
            }
        } else {
            throw new RuntimeException("Initialize must be called before run.");
        }
    }

    /**
     * Setup necessary AWS resources for the samples. By default, the Executor does not create any
     * AWS resources. The user must specify true for the specific create properties in the
     * configuration file.
     */
    private void setupAWSResources() {
        createS3Bucket(s3Bucket);
    }

    Metastore getMetastore() {
        return metastore;
    }

    /**
     * Helper method to create the Amazon S3 bucket.
     * 
     * @param s3Bucket
     *        The name of the bucket to create
     */
    private void createS3Bucket(String s3Bucket) {
        AmazonS3Client client = new AmazonS3Client(config.AWS_CREDENTIALS_PROVIDER);
        client.setEndpoint(config.S3_ENDPOINT);
        LOG.info("Creating Amazon S3 bucket " + s3Bucket);
        S3Utils.createBucket(client, s3Bucket);
    }

    public IRecordProcessorFactory getKinesisConnectorRecordProcessorFactory() {
        return () -> new AWSKinesisConsumer(config, metastore);
    }
}
