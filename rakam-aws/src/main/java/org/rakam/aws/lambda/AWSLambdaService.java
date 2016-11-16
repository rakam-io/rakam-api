package org.rakam.aws.lambda;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClient;
import com.amazonaws.services.cloudwatchevents.model.PutRuleRequest;
import com.amazonaws.services.cloudwatchevents.model.PutTargetsRequest;
import com.amazonaws.services.cloudwatchevents.model.RuleState;
import com.amazonaws.services.cloudwatchevents.model.Target;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.CreateFunctionRequest;
import com.amazonaws.services.lambda.model.FunctionCode;
import com.amazonaws.services.lambda.model.Runtime;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import org.rakam.aws.AWSConfig;

import javax.inject.Inject;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class AWSLambdaService
{
    private final AWSLambdaClient client;
    private final AWSLambdaConfig awsLambdaConfig;
    private final AmazonS3Client awsS3Client;

    public static void main(String[] args)
    {
        AWSConfig awsConfig = new AWSConfig().setAccessKey("AKIAIDL7OPDZWNCS6Z2A").setSecretAccessKey("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");

        AWSLambdaService awsLambdaService = new AWSLambdaService(awsConfig, new AWSLambdaConfig());
//        String taskArn = awsLambdaService.createTask();

        AmazonCloudWatchEventsClient amazonCloudWatchEventsClient = new AmazonCloudWatchEventsClient(awsConfig.getCredentials());

        amazonCloudWatchEventsClient.putRule(new PutRuleRequest()
                .withName("test")
                .withState(RuleState.ENABLED)
                .withScheduleExpression("rate(1 hour)"));

        amazonCloudWatchEventsClient.putTargets(new PutTargetsRequest()
                .withRule("test")
                .withTargets(new Target()
                        .withId("aheyahey")
//                        .withArn(taskArn)
                        .withInput("{\"a\": 1}")));
    }

    private String createTaskFromMarket(String name)
    {
        ObjectMetadata objectMetadata = awsS3Client.getObjectMetadata(awsLambdaConfig.getMarketS3Bucket(), name);
        String description = objectMetadata.getUserMetaDataOf("description");
        String handler = objectMetadata.getUserMetaDataOf("handler");
        String language = objectMetadata.getUserMetaDataOf("language");
        int memory = Optional.ofNullable(objectMetadata.getUserMetaDataOf("memory"))
                .map(e -> Integer.valueOf(e))
                .orElse(128);

        return client.createFunction(new CreateFunctionRequest()
                .withFunctionName(name)
                .withDescription(description)
                .withHandler(handler)
                .withMemorySize(memory)
                .withRuntime(Runtime.valueOf(language))
                .withPublish(true).withRole("role").withTimeout(5000)
                .withCode(new FunctionCode()
                        .withS3Bucket(awsLambdaConfig.getMarketS3Bucket())
                        .withS3Key(name)))
                .getFunctionArn();
    }

    @Inject
    public AWSLambdaService(AWSConfig awsConfig, AWSLambdaConfig awsLambdaConfig)
    {
        this.awsLambdaConfig = awsLambdaConfig;

        client = new AWSLambdaClient(awsConfig.getCredentials());
        awsS3Client = new AmazonS3Client(awsConfig.getCredentials());

        client.setRegion(awsConfig.getAWSRegion());
        if (awsConfig.getLambdaEndpoint() != null) {
            client.setEndpoint(awsConfig.getLambdaEndpoint());
        }
    }

    public List<Task> getActiveTasks()
    {
        return client.listFunctions().getFunctions().stream().map(e ->
                new Task(e.getFunctionName(), e.getDescription(), e.getVersion(), Instant.parse(e.getLastModified())))
                .collect(Collectors.toList());
    }

    public List<Task> activateTask()
    {
        return client.listFunctions().getFunctions().stream().map(e ->
                new Task(e.getFunctionName(), e.getDescription(), e.getVersion(), Instant.parse(e.getLastModified())))
                .collect(Collectors.toList());
    }

    public static class TaskList
    {
        public final List<Task> task;
        public final String marker;

        public TaskList(List<Task> task, String marker)
        {
            this.task = task;
            this.marker = marker;
        }
    }

    public TaskList listTasksFromMarket(Optional<String> marker)
    {
        ObjectListing objectListing = awsS3Client.listObjects(new ListObjectsRequest()
                .withBucketName(awsLambdaConfig.getMarketS3Bucket())
                .withMarker(marker.orElse(null)));

        ImmutableList.Builder<Task> builder = ImmutableList.builder();
        for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
            Map<String, String> userMetadata = awsS3Client.getObjectMetadata(awsLambdaConfig.getMarketS3Bucket(), s3ObjectSummary.getKey()).getUserMetadata();
            String description = userMetadata.get("description");
            String version = userMetadata.get("version");
            builder.add(new Task(s3ObjectSummary.getKey(), description, version,
                    s3ObjectSummary.getLastModified().toInstant()));
        }

        return new TaskList(builder.build(), objectListing.getNextMarker());
    }

    public static class Task
    {
        public final String name;
        public final String description;
        public final String version;
        public final Instant createdAt;

        public Task(String name, String description, String version, Instant createdAt)
        {
            this.name = name;
            this.description = description;
            this.version = version;
            this.createdAt = createdAt;
        }
    }
}
