package org.rakam.aws.lambda;

import io.airlift.configuration.Config;

public class AWSLambdaConfig
{
    String marketS3Bucket = "rakam-task-market";

    @Config("task.market_s3_bucket")
    public AWSLambdaConfig setMarketS3Bucket(String marketS3Bucket)
    {
        this.marketS3Bucket = marketS3Bucket;
        return this;
    }

    public String getMarketS3Bucket()
    {
        return marketS3Bucket;
    }
}
