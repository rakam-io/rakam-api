package org.rakam.aws.dynamodb.metastore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.aws.AWSConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.util.JsonHelper;
import org.rakam.util.NotExistsException;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.of;

public class DynamodbQueryMetastore
        implements QueryMetadataStore {
    private static final List<KeySchemaElement> PROJECT_KEYSCHEMA = ImmutableList.of(
            new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName("project"),
            new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName("type_table_name")
    );
    private static final Set<AttributeDefinition> ATTRIBUTES = ImmutableSet.of(
            new AttributeDefinition().withAttributeName("project").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("type_table_name").withAttributeType(ScalarAttributeType.S)
    );

    private final AmazonDynamoDBClient dynamoDBClient;
    private final DynamodbQueryMetastoreConfig tableConfig;

    @Inject
    public DynamodbQueryMetastore(AWSConfig config, DynamodbQueryMetastoreConfig tableConfig) {
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
                throw new IllegalStateException("Dynamodb table for query metadata store has invalid key schema");
            }

            if (!ImmutableSet.copyOf(table.getTable().getAttributeDefinitions()).equals(ATTRIBUTES)) {
                throw new IllegalStateException("Dynamodb table for query metadata store has invalid attribute schema");
            }
        } catch (ResourceNotFoundException e) {
            createTable();
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
    public void createMaterializedView(String project, MaterializedView materializedView) {
        dynamoDBClient.putItem(new PutItemRequest().withTableName(tableConfig.getTableName())
                .withItem(of(
                        "project", new AttributeValue(project),
                        "type_table_name", new AttributeValue("materialized_" + materializedView.tableName),
                        "value", new AttributeValue(JsonHelper.encode(materializedView)))));
    }

    @Override
    public void deleteMaterializedView(String project, String tableName) {
        dynamoDBClient.deleteItem(new DeleteItemRequest().withTableName(tableConfig.getTableName())
                .withKey(of(
                        "project", new AttributeValue(project),
                        "type_table_name", new AttributeValue("materialized_" + tableName))));
    }

    @Override
    public MaterializedView getMaterializedView(String project, String tableName) {
        Map<String, AttributeValue> item = dynamoDBClient.getItem(new GetItemRequest().withTableName(tableConfig.getTableName())
                .withAttributesToGet("value")
                .withKey(of(
                        "project", new AttributeValue(project),
                        "type_table_name", new AttributeValue("materialized_" + tableName)))).getItem();
        if (item == null) {
            throw new NotExistsException("Materialized view");
        }

        return JsonHelper.read(item.get("value").getS(), MaterializedView.class);
    }

    @Override
    public List<MaterializedView> getMaterializedViews(String project) {
        List<Map<String, AttributeValue>> items = dynamoDBClient.scan(new ScanRequest()
                .withTableName(tableConfig.getTableName())
                .withFilterExpression("#P = :pValue AND begins_with(type_table_name, :prefix)")
                .withExpressionAttributeNames(of("#P", "project"))
                .withExpressionAttributeValues(of(
                        ":pValue", new AttributeValue(project),
                        ":prefix", new AttributeValue("materialized_")))).getItems();
        return items.stream()
                .map(item -> JsonHelper.read(item.get("value").getS(), MaterializedView.class))
                .collect(Collectors.toList());
    }

    @Override
    public boolean updateMaterializedView(String project, MaterializedView view, CompletableFuture<Instant> releaseLock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeMaterializedView(String project, String tableName, boolean realTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alter(String project, MaterializedView view) {
        throw new UnsupportedOperationException();
    }
}
