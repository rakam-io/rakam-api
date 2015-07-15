package org.rakam.analysis;

import io.airlift.configuration.Config;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 06:48.
 */
public class AWSConfig {
    private String accessKey;
    private String secretAccessKey;
    private boolean kinesisWorker;
    private String s3Bucket;
    private String streamName;
    private String kinesisCollectionStreamPrefix;
    private String kinesisStream;

    @Config("aws.access_key")
    public AWSConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    @Config("aws.event.manifest.stream_name")
    public AWSConfig setManifestStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public String getKinesisStream() {
        return kinesisStream;
    }

    public String getManifestStreamName() {
        return streamName;
    }

    @Config("aws.kinesis.worker")
    public AWSConfig setKinesisWorker(boolean kinesisWorker) {
        this.kinesisWorker = kinesisWorker;
        return this;
    }

    @Config("aws.event.s3_bucket")
    public AWSConfig setS3Bucket(String S3Bucket) {
        this.s3Bucket = S3Bucket;
        return this;
    }

    @Config("aws.event.kinesis.stream")
    public AWSConfig setKinesisStream(String kinesisStream) {
        this.kinesisStream = kinesisStream;
        return this;
    }



    public String getS3Bucket() {
        return s3Bucket;
    }

    public boolean getKinesisWorker() {
        return kinesisWorker;
    }

    public String getAccessKey() {
        return accessKey;
    }

    @Config("aws.secret_access_key")
    public AWSConfig setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }
}
