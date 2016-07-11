package org.rakam.aws.dynamodb.apikey;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ApiKeyService;
import org.rakam.aws.AWSConfig;
import org.rakam.util.CryptUtil;
import org.rakam.util.RakamException;

import javax.annotation.PostConstruct;

import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static java.lang.String.format;

public class DynamodbApiKeyService
        implements ApiKeyService
{
    private final AmazonDynamoDBClient dynamoDBClient;
    private static final List<KeySchemaElement> PROJECT_KEYSCHEMA = ImmutableList.of(
            new KeySchemaElement().withKeyType(HASH).withAttributeName("project")
    );
    private static final List<AttributeDefinition> ATTRIBUTES = ImmutableList.of(
            new AttributeDefinition().withAttributeName("project").withAttributeType(ScalarAttributeType.S)
    );
    private final DynamodbApiKeyConfig apiKeyConfig;

    @Inject
    public DynamodbApiKeyService(AWSConfig config, DynamodbApiKeyConfig apiKeyConfig)
    {
        dynamoDBClient = new AmazonDynamoDBClient(config.getCredentials());
        if (config.getDynamodbEndpoint() != null) {
            dynamoDBClient.setEndpoint(config.getDynamodbEndpoint());
        }
        this.apiKeyConfig = apiKeyConfig;
    }

    @PostConstruct
    public void setup()
    {
        try {
            DescribeTableResult table = dynamoDBClient.describeTable(apiKeyConfig.getTableName());

            if (!table.getTable().getKeySchema().equals(PROJECT_KEYSCHEMA)) {
                throw new IllegalStateException("Dynamodb table for api key service has invalid key schema");
            }

            if (!table.getTable().getAttributeDefinitions().equals(ATTRIBUTES)) {
                throw new IllegalStateException("Dynamodb table for api key service has invalid attribute schema");
            }
        }
        catch (ResourceNotFoundException e) {
            createTable();
        }
    }

    private void createTable()
    {
        dynamoDBClient.createTable(new CreateTableRequest()
                .withTableName(apiKeyConfig.getTableName()).withKeySchema(PROJECT_KEYSCHEMA)
                .withAttributeDefinitions(ATTRIBUTES)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(1L)
                        .withWriteCapacityUnits(1L)));
    }

    @Override
    public ProjectApiKeys createApiKeys(String project)
    {
        String masterKey = CryptUtil.generateRandomKey(64);
        String readKey = CryptUtil.generateRandomKey(64);
        String writeKey = CryptUtil.generateRandomKey(64);

        dynamoDBClient.putItem(new PutItemRequest().withTableName(apiKeyConfig.getTableName())
                .withItem(ImmutableMap.of("project", new AttributeValue(project), "keys",
                        new AttributeValue()
                                .addMEntry("master_key", new AttributeValue(masterKey))
                                .addMEntry("read_key", new AttributeValue(readKey))
                                .addMEntry("write_key", new AttributeValue(writeKey))
                )));

        return ProjectApiKeys.create(masterKey, readKey, writeKey);
    }

    @Override
    public String getProjectOfApiKey(String apiKey, AccessKeyType type)
    {
        List<Map<String, AttributeValue>> project = dynamoDBClient.scan(new ScanRequest()
                .withTableName(apiKeyConfig.getTableName())
                .withConsistentRead(true)
                .withProjectionExpression("#P")
                .withExpressionAttributeNames(ImmutableMap.of("#K", "keys", "#P", "project"))
                .withExpressionAttributeValues(ImmutableMap.of(":apiKey", new AttributeValue(apiKey)))
                .withFilterExpression(String.format("#K.%s = :apiKey", type.getKey()))).getItems();
        if (project.isEmpty()) {
            throw new RakamException(HttpResponseStatus.FORBIDDEN);
        }

        return project.get(0).get("project").getS();
    }

    @Override
    public void revokeApiKeys(String project, String masterKey)
    {
        dynamoDBClient.deleteItem(new DeleteItemRequest()
                .withTableName(apiKeyConfig.getTableName())
                .withKey(ImmutableMap.of("project", new AttributeValue(project)))
                .withExpressionAttributeNames(ImmutableMap.of("#K", format("keys")))
                .withExpressionAttributeValues(ImmutableMap.of(":V", new AttributeValue(masterKey)))
                .withConditionExpression(format("#K.master_key = :V", masterKey)));
    }

    @Override
    public void revokeAllKeys(String project)
    {
        dynamoDBClient.deleteItem(new DeleteItemRequest().withTableName(apiKeyConfig.getTableName())
                .withKey(ImmutableMap.of("project", new AttributeValue(project))));
    }
}
