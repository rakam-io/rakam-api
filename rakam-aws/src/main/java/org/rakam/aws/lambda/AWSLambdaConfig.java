package org.rakam.aws.lambda;

import io.airlift.configuration.Config;

public class AWSLambdaConfig
{
    String marketS3Bucket = "rakam-task-market";
    private String roleArn;
    private String stackId;

    @Config("task.market_s3_bucket")
    public AWSLambdaConfig setMarketS3Bucket(String marketS3Bucket)
    {
        this.marketS3Bucket = marketS3Bucket;
        return this;
    }

    @Config("task.role_arn")
    public AWSLambdaConfig setRoleArn(String roleArn)
    {
        this.roleArn = roleArn;
        return this;
    }

    public String getRoleArn()
    {
        return roleArn;
    }

    @Config("task.stack_id")
    public AWSLambdaConfig setStackId(String stackId)
    {
        this.stackId = stackId;
        return this;
    }

    public String getStackId()
    {
        return stackId;
    }

    public String getMarketS3Bucket()
    {
        return marketS3Bucket;
    }
}
