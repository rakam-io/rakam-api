package org.rakam.aws.dynamodb.config;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.rakam.analysis.ConfigManager;
import org.rakam.aws.AWSConfig;
import org.rakam.util.JsonHelper;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotNull;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.of;

public class DynamodbConfigManager
        implements ConfigManager {
    private static final List<KeySchemaElement> PROJECT_KEYSCHEMA = ImmutableList.of(
            new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName("project"),
            new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName("id")
    );
    private static final Set<AttributeDefinition> ATTRIBUTES = ImmutableSet.of(
            new AttributeDefinition().withAttributeName("project").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S)
    );
    private final AmazonDynamoDBClient dynamoDBClient;
    private final DynamodbConfigManagerConfig tableConfig;

    @Inject
    public DynamodbConfigManager(AWSConfig config, DynamodbConfigManagerConfig tableConfig) {
        dynamoDBClient = new AmazonDynamoDBClient(config.getCredentials());
        dynamoDBClient.setRegion(config.getAWSRegion());
        if (config.getDynamodbEndpoint() != null) {
            dynamoDBClient.setEndpoint(config.getDynamodbEndpoint());
        }
        this.tableConfig = tableConfig;
    }

    @PostConstruct
    public void setup() {
        try {
            DescribeTableResult table = dynamoDBClient.describeTable(tableConfig.getTableName());

            if (!table.getTable().getKeySchema().equals(PROJECT_KEYSCHEMA)) {
                throw new IllegalStateException("Dynamodb table for config manager has invalid key schema");
            }

            if (!ImmutableSet.copyOf(table.getTable().getAttributeDefinitions()).equals(ATTRIBUTES)) {
                throw new IllegalStateException("Dynamodb table for config manager has invalid attribute schema.");
            }
        } catch (ResourceNotFoundException e) {
            createTable();
        }
    }

    @Override
    public <T> T getConfig(String project, String configName, Class<T> clazz) {
        Map<String, AttributeValue> item = dynamoDBClient.getItem(new GetItemRequest()
                .withTableName(tableConfig.getTableName())
                .withKey(new SimpleImmutableEntry<>("project", new AttributeValue(project)),
                        new SimpleImmutableEntry<>("id", new AttributeValue(configName)))
                .withAttributesToGet("value")
                .withConsistentRead(true)).getItem();
        if (item == null) {
            return null;
        }
        String value = item.get("value").getS();
        return value == null ? null : JsonHelper.read(value, clazz);
    }

    @Override
    public <T> void setConfig(String project, String configName, @NotNull T value) {
        dynamoDBClient.putItem(new PutItemRequest()
                .withTableName(tableConfig.getTableName())
                .withItem(of(
                        "project", new AttributeValue(project),
                        "id", new AttributeValue(configName),
                        "value", new AttributeValue(JsonHelper.encode(value)))));
    }

    @Override
    public <T> T setConfigOnce(String project, String configName, @NotNull T value) {
        try {
            dynamoDBClient.putItem(new PutItemRequest()
                    .withTableName(tableConfig.getTableName())
                    .withExpected(of("id", new ExpectedAttributeValue(false), "project", new ExpectedAttributeValue(false)))
                    .withItem(of(
                            "project", new AttributeValue(project),
                            "id", new AttributeValue(configName),
                            "value", new AttributeValue(JsonHelper.encode(value)))));

            return value;
        } catch (Exception e) {
            return getConfig(project, configName, (Class<T>) value.getClass());
        }
    }

    private void createTable() {
        dynamoDBClient.createTable(new CreateTableRequest()
                .withTableName(tableConfig.getTableName()).withKeySchema(PROJECT_KEYSCHEMA)
                .withAttributeDefinitions(ATTRIBUTES)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(1L)
                        .withWriteCapacityUnits(1L)));
    }

    @Override
    public void clear() {
        dynamoDBClient.deleteTable(tableConfig.getTableName());
        createTable();
    }
}
