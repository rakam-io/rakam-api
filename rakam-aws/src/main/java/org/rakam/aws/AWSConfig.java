package org.rakam.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.airlift.configuration.Config;

public class AWSConfig {
    private String accessKey;
    private String secretAccessKey;
    private String eventStoreStreamName;
    private String region;
    private String eventStoreBulkS3Bucket;
    private String s3Endpoint;
    private String kinesisEndpoint;
    private String dynamodbEndpoint;
    private String lambdaEndpoint;

    public String getEventStoreStreamName() {
        return eventStoreStreamName;
    }

    @Config("event.store.kinesis.stream")
    public AWSConfig setEventStoreStreamName(String eventStoreStreamName) {
        this.eventStoreStreamName = eventStoreStreamName;
        return this;
    }

    public String getEventStoreBulkS3Bucket() {
        return eventStoreBulkS3Bucket;
    }

    @Config("event.store.bulk.s3-bucket")
    public AWSConfig setEventStoreBulkS3Bucket(String eventStoreBulkS3Bucket) {
        this.eventStoreBulkS3Bucket = eventStoreBulkS3Bucket;
        return this;
    }

    public String getAccessKey() {
        return accessKey;
    }

    @Config("aws.access-key")
    public AWSConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public String getRegion() {
        return region;
    }

    @Config("aws.region")
    public AWSConfig setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    @Config("aws.s3-endpoint")
    public AWSConfig setS3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public String getKinesisEndpoint() {
        return kinesisEndpoint;
    }

    @Config("aws.kinesis-endpoint")
    public AWSConfig setKinesisEndpoint(String kinesisEndpoint) {
        this.kinesisEndpoint = kinesisEndpoint;
        return this;
    }

    public String getLambdaEndpoint() {
        return lambdaEndpoint;
    }

    @Config("aws.lambda-endpoint")
    public AWSConfig setLambdaEndpoint(String lambdaEndpoint) {
        this.lambdaEndpoint = lambdaEndpoint;
        return this;
    }

    public Region getAWSRegion() {
        return Region.getRegion(region == null || region.isEmpty() ? Regions.DEFAULT_REGION : Regions.fromName(region));
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    @Config("aws.secret-access-key")
    public AWSConfig setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public AWSCredentialsProvider getCredentials() {
        // TODO: add an extra option the allow these values to be NULL.
        if (accessKey == null || secretAccessKey == null) {
            return new DefaultAWSCredentialsProviderChain();
        }

        return new StaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }

    public String getDynamodbEndpoint() {
        return dynamodbEndpoint;
    }

    @Config("aws.dynamodb-endpoint")
    public AWSConfig setDynamodbEndpoint(String dynamodbEndpoint) {
        this.dynamodbEndpoint = dynamodbEndpoint;
        return this;
    }
}
