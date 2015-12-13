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


    public String getEventStoreStreamName() {
        return eventStoreStreamName;
    }

    @Config("event.store.kinesis.stream")
    public void setEventStoreStreamName(String eventStoreStreamName) {
        this.eventStoreStreamName = eventStoreStreamName;
    }

    @Config("aws.access_key")
    public AWSConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public String getAccessKey() {
        return accessKey;
    }

    @Config("aws.secret_access_key")
    public AWSConfig setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    @Config("aws.region")
    public AWSConfig setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public Region getAWSRegion() {
        return Region.getRegion(region == null ? Regions.DEFAULT_REGION : Regions.fromName(region));
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public AWSCredentialsProvider getCredentials() {
        // TODO: add an extra option the allow these values to be NULL.
        if(accessKey == null || secretAccessKey == null) {
            return new DefaultAWSCredentialsProviderChain();
        }

        return new StaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }
}
