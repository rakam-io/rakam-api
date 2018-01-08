package org.rakam.aws.dynamodb.metastore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.aws.AWSConfig;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;

public class DynamodbMetastore
        extends AbstractMetastore {
    private static final List<KeySchemaElement> PROJECT_KEYSCHEMA = ImmutableList.of(
            new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName("project"),
            new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName("id")
    );
    private static final Set<AttributeDefinition> ATTRIBUTES = ImmutableSet.of(
            new AttributeDefinition().withAttributeName("project").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S)
    );
    private final AmazonDynamoDBClient dynamoDBClient;
    private final DynamodbMetastoreConfig tableConfig;

    @Inject
    public DynamodbMetastore(AWSConfig config, DynamodbMetastoreConfig tableConfig, FieldDependencyBuilder.FieldDependency fieldDependency, EventBus eventBus) {
        super(eventBus);
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
                .withStreamSpecification(new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_IMAGE))
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(1L)
                        .withWriteCapacityUnits(1L)));
    }

    @Override
    public List<SchemaField> getOrCreateCollectionFields(String project, String collection, Set<SchemaField> newFields) {
        ValidationUtil.checkCollectionValid(collection);

        List<SchemaField> fields = getCollection(project, collection);
        int i = 0;
        for (SchemaField newField : newFields) {
            Optional<SchemaField> existing = fields.stream().filter(e -> e.getName().equals(newField.getName())).findAny();
            if (existing.isPresent()) {
                if (!existing.get().getType().equals(newField.getType())) {
                    throw new IllegalStateException(String.format("Multiple entries with same key for collection %s field %s: %s,%s", collection, newField.getName(), newField.getType(), existing.get().getType()));
                }

                continue;
            }

            String rangeKey = collection + "|" + String.format("%06d", (fields.size() + i++));
            try {
                dynamoDBClient.putItem(new PutItemRequest()
                        .withTableName(tableConfig.getTableName())
                        .addExpectedEntry("id", new ExpectedAttributeValue().withExists(false))
                        .withItem(ImmutableMap.<String, AttributeValue>builder()
                                .put("project", new AttributeValue(project))
                                .put("id", new AttributeValue(rangeKey))
                                .put("collection", new AttributeValue(collection)).put("name", new AttributeValue(newField.getName()))
                                .put("type", new AttributeValue(newField.getType().name())).build())
                );
                fields.add(newField);
            } catch (ConditionalCheckFailedException e) {
                boolean isDone = false;
                for (int i1 = 0; i1 < 5000; i1++) {
                    rangeKey = collection + "|" + String.format("%06d", (fields.size() + i++));

                    try {
                        dynamoDBClient.putItem(new PutItemRequest()
                                .withTableName(tableConfig.getTableName())
                                .addExpectedEntry("id", new ExpectedAttributeValue().withExists(false))
                                .withItem(ImmutableMap.<String, AttributeValue>builder()
                                        .put("project", new AttributeValue(project))
                                        .put("id", new AttributeValue(rangeKey))
                                        .put("collection", new AttributeValue(collection)).put("name", new AttributeValue(newField.getName()))
                                        .put("type", new AttributeValue(newField.getType().name())).build())
                        );
                    } catch (ConditionalCheckFailedException e1) {
                        continue;
                    }

                    fields.add(newField);
                    isDone = true;
                    break;
                }

                if (!isDone) {
                    throw new RakamException("Unable to add new field", HttpResponseStatus.BAD_REQUEST);
                }
            }
        }

        return getCollection(project, collection);
    }

    @Override
    public Map<String, List<SchemaField>> getCollections(String project) {
        QueryResult query = dynamoDBClient.query(new QueryRequest()
                .withTableName(tableConfig.getTableName())
                .withKeyConditions(ImmutableMap.of("project", new Condition()
                        .withComparisonOperator(EQ)
                        .withAttributeValueList(new AttributeValue(project)))));

        Map<String, List<SchemaField>> builder = new HashMap();
        for (Map<String, AttributeValue> entry : query.getItems()) {
            if (entry.get("id").getS().equals("|")) {
                continue;
            }
            builder.computeIfAbsent(entry.get("collection").getS(),
                    (k) -> new ArrayList<>()).add(new SchemaField(
                    entry.get("name").getS(),
                    FieldType.valueOf(entry.get("type").getS())));
        }

        return builder;
    }

    @Override
    public Set<String> getCollectionNames(String project) {
        return getCollections(project).keySet();
    }

    @Override
    public void createProject(String project) {
        try {
            dynamoDBClient.putItem(new PutItemRequest()
                    .withTableName(tableConfig.getTableName())
                    .addExpectedEntry("id", new ExpectedAttributeValue().withExists(false))
                    .withItem(ImmutableMap.<String, AttributeValue>builder()
                            .put("project", new AttributeValue(project))
                            .put("id", new AttributeValue("|")).build()
                    ));
        } catch (ConditionalCheckFailedException e) {
            throw new AlreadyExistsException("Project", HttpResponseStatus.BAD_REQUEST);
        }
    }

    @Override
    public Set<String> getProjects() {
        return dynamoDBClient.scan(new ScanRequest().withTableName(tableConfig.getTableName())
                .withAttributesToGet("project")).getItems().stream()
                .map(e -> e.get("project").getS())
                .collect(Collectors.toSet());
    }

    @Override
    public List<SchemaField> getCollection(String project, String collection) {
        QueryResult query = dynamoDBClient.query(new QueryRequest()
                .withTableName(tableConfig.getTableName())
                .withKeyConditions(ImmutableMap.of("project", new Condition().withComparisonOperator(EQ).withAttributeValueList(new AttributeValue(project))))
                .withQueryFilter(
                        ImmutableMap.of("collection",
                                new Condition().withComparisonOperator(EQ)
                                        .withAttributeValueList(new AttributeValue(collection)))));

        return query.getItems().stream()
                .filter(e -> !e.get("id").getS().equals("|"))
                .map(e -> new SchemaField(e.get("name").getS(),
                        FieldType.valueOf(e.get("type").getS())))
                .collect(Collectors.toList());
    }

    @Override
    public void deleteProject(String project) {
        dynamoDBClient.deleteItem(new DeleteItemRequest()
                .withTableName(tableConfig.getTableName())
                .withKey(
                        new AbstractMap.SimpleEntry<>("project", new AttributeValue(project)),
                        new AbstractMap.SimpleEntry<>("id", new AttributeValue("|"))));

        for (int i = 0; i < 100; i++) {
            List<Map<String, AttributeValue>> items = dynamoDBClient.query(new QueryRequest()
                    .withTableName(tableConfig.getTableName())
                    .withKeyConditions(ImmutableMap.of("project",
                            new Condition().withComparisonOperator(EQ)
                                    .withAttributeValueList(new AttributeValue(project))))).getItems();
            if (items == null || items.isEmpty()) {
                return;
            }
            for (Map<String, AttributeValue> item : items) {
                dynamoDBClient.deleteItem(new DeleteItemRequest()
                        .withTableName(tableConfig.getTableName())
                        .withKey(
                                new AbstractMap.SimpleEntry<>("project", new AttributeValue(project)),
                                new AbstractMap.SimpleEntry<>("id", new AttributeValue(item.get("id").getS()))));
            }
        }

        throw new RakamException("Unable to delete project", HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    @Override
    public CompletableFuture<List<String>> getAttributes(String project, String collection, String attribute, Optional<LocalDate> startDate,
                                                         Optional<LocalDate> endDate, Optional<String> filter) {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    public void deleteTable() {
        dynamoDBClient.deleteTable(tableConfig.getTableName());
    }
}
