package org.rakam.aws.lambda;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClient;
import com.amazonaws.services.cloudwatchevents.model.ListRuleNamesByTargetRequest;
import com.amazonaws.services.cloudwatchevents.model.ListRuleNamesByTargetResult;
import com.amazonaws.services.cloudwatchevents.model.ListRulesRequest;
import com.amazonaws.services.cloudwatchevents.model.ListTargetsByRuleRequest;
import com.amazonaws.services.cloudwatchevents.model.ListTargetsByRuleResult;
import com.amazonaws.services.cloudwatchevents.model.PutRuleRequest;
import com.amazonaws.services.cloudwatchevents.model.PutTargetsRequest;
import com.amazonaws.services.cloudwatchevents.model.RuleState;
import com.amazonaws.services.cloudwatchevents.model.Target;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.CreateFunctionRequest;
import com.amazonaws.services.lambda.model.FunctionCode;
import com.amazonaws.services.lambda.model.ListFunctionsRequest;
import com.amazonaws.services.lambda.model.ListFunctionsResult;
import com.amazonaws.services.lambda.model.Runtime;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.rakam.aws.AWSConfig;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AWSLambdaService
{
    private final AWSLambdaClient client;
    private final AWSLambdaConfig awsLambdaConfig;
    private final AmazonS3Client awsS3Client;
    private final AmazonCloudWatchEventsClient CWEventsClient;

    public static void main(String[] args)
    {
        AmazonCloudWatchEventsClient amazonCloudWatchEventsClient = new AmazonCloudWatchEventsClient();

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
        String language = objectMetadata.getUserMetaDataOf("runtime");
        int memory = Optional.ofNullable(objectMetadata.getUserMetaDataOf("memory"))
                .map(e -> Integer.valueOf(e))
                .orElse(128);

        return client.createFunction(new CreateFunctionRequest()
                .withFunctionName(name)
                .withDescription(description)
                .withHandler(handler)
                .withMemorySize(memory)
                .withRuntime(Runtime.fromValue(language))
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
        CWEventsClient = new AmazonCloudWatchEventsClient(awsConfig.getCredentials());

        client.setRegion(awsConfig.getAWSRegion());
        if (awsConfig.getLambdaEndpoint() != null) {
            client.setEndpoint(awsConfig.getLambdaEndpoint());
        }
    }

    public List<Task> getActiveTasks()
    {
        ListFunctionsResult listFunctionsResult = client
                .listFunctions(new ListFunctionsRequest().withMarker(null));

        CWEventsClient.listTargetsByRule(new ListTargetsByRuleRequest().withRule(null)).getTargets().get(0).getInput();

        CWEventsClient
                .listRules(new ListRulesRequest().withNamePrefix(awsLambdaConfig.getStackId()))
                .getRules().stream()
                .flatMap(e -> {
                    ListTargetsByRuleResult listTargetsByRuleResult = CWEventsClient.listTargetsByRule(new ListTargetsByRuleRequest().withRule(e.getName()));
                    return listTargetsByRuleResult.getTargets().stream().map(a -> {
                        return new Task(e.getName(), e.getDescription(), a.getArn(), null, null);
                    });
                });

        return listFunctionsResult
                .getFunctions()
                .stream()
                .filter(e -> e.getRole().equals(awsLambdaConfig.getRoleArn()))
                .flatMap(e -> {
                    ListRuleNamesByTargetResult listRuleNamesByTargetResult = CWEventsClient.listRuleNamesByTarget(new ListRuleNamesByTargetRequest().withTargetArn(e.getFunctionArn()));
                    if(listRuleNamesByTargetResult.getRuleNames().isEmpty()) {
                        return Stream.of();
                    }

                    String ruleName = listRuleNamesByTargetResult.getRuleNames().get(0);
                    Target target = CWEventsClient
                            .listTargetsByRule(new ListTargetsByRuleRequest().withRule(ruleName))
                            .getTargets()
                            .get(0);

                    Map<String, String> read = JsonHelper.read(target.getInput(), new TypeReference<Map<String, String>>() {});

                    return listRuleNamesByTargetResult.getRuleNames().stream()
                            .map(taskName -> {
                                return new Task(taskName, e.getDescription(), e.getVersion(), Runtime.fromValue(e.getRuntime()), e.getHandler());
                            });
                })
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
            String runtime = userMetadata.get("runtime");
            String handler = userMetadata.get("handler");
            builder.add(new Task(
                    s3ObjectSummary.getKey(),
                    description,
                    version,
                    Runtime.fromValue(runtime),
                    handler));
        }

        return new TaskList(builder.build(), objectListing.getNextMarker());
    }

    public static class Task
    {
        public final String name;
        public final String description;
        public final String version;
        public final Runtime runtime;
        public final String handler;

        public Task(String name, String description, String version, Runtime runtime, String handler)
        {
            this.name = name;
            this.description = description;
            this.version = version;
            this.runtime = runtime;
            this.handler = handler;
        }
    }
}
