package org.rakam.aws.lambda;

import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.CreateFunctionRequest;
import com.amazonaws.services.lambda.model.FunctionCode;
import com.amazonaws.services.lambda.model.Runtime;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
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

    public List<Task> getTaskList()
    {
        return client.listFunctions().getFunctions().stream().map(e ->
                new Task(e.getFunctionName(), e.getDescription(), e.getVersion(), Instant.parse(e.getLastModified())))
                .collect(Collectors.toList());
    }

    public void addTask()
    {
        client.createFunction(new CreateFunctionRequest()
                .withCode(new FunctionCode().withS3Bucket("test").withS3Key(""))
                .withFunctionName("").withDescription("").withHandler("")
                .withPublish(true).withRuntime(Runtime.Nodejs).withTimeout(100000));
    }

    public static class TaskList {
        public final List<Task> task;
        public final String marker;

        public TaskList(List<Task> task, String marker) {
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
