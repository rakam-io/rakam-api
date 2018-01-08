package org.rakam.aws.dynamodb.user;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.RequestContext;
import org.rakam.aws.AWSConfig;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.AbstractUserService.BatchUserOperationRequest.BatchUserOperations;
import org.rakam.plugin.user.ISingleUserBatchOperation;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryResult;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static org.rakam.collection.FieldType.STRING;

public class DynamodbUserStorage
        implements UserStorage {
    private static final List<KeySchemaElement> PROJECT_KEYSCHEMA = ImmutableList.of(
            new KeySchemaElement().withKeyType(HASH).withAttributeName("project"),
            new KeySchemaElement().withKeyType(RANGE).withAttributeName("id")
    );
    private static final Set<AttributeDefinition> ATTRIBUTES = ImmutableSet.of(
            new AttributeDefinition().withAttributeName("project").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S)
    );
    private final AmazonDynamoDBClient dynamoDBClient;
    private final DynamodbUserConfig tableConfig;

    @Inject
    public DynamodbUserStorage(AWSConfig config, DynamodbUserConfig tableConfig) {
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
            DescribeTableResult result = dynamoDBClient.describeTable(tableConfig.getTableName());

            if (!result.getTable().getKeySchema().equals(PROJECT_KEYSCHEMA) || !ImmutableSet.copyOf(result.getTable().getAttributeDefinitions()).equals(ATTRIBUTES)) {
                throw new IllegalStateException("Invalid schema for user storage dynamodb table. " +
                        "Please remove existing table or change dynamodb table.");
            }
        } catch (ResourceNotFoundException e) {
            dynamoDBClient.createTable(new CreateTableRequest().withTableName(tableConfig.getTableName())
                    .withKeySchema(PROJECT_KEYSCHEMA)
                    .withAttributeDefinitions(ATTRIBUTES)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(3L)
                            .withWriteCapacityUnits(3L)));
        }
    }

    @Override
    public Object create(String project, Object id, ObjectNode properties) {
        dynamoDBClient.putItem(new PutItemRequest().withTableName(tableConfig.getTableName())
                .withItem(generatePutRequest(project, id, properties)));
        return id;
    }

    private Map<String, AttributeValue> generatePutRequest(String project, Object id, ObjectNode properties) {
        Map<String, AttributeValue> builder = new HashMap<>();
        builder.put("project", new AttributeValue(project));
        builder.put("id", new AttributeValue(id.toString()));

        Map<String, AttributeValue> props = new HashMap<>();
        Iterator<Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            Entry<String, JsonNode> next = fields.next();

            props.put(next.getKey(), convertAttributeValue(next.getValue()));
        }

        builder.put("properties", new AttributeValue().withM(props));

        return builder;
    }

    private AttributeValue convertAttributeValue(JsonNode value) {
        AttributeValue attr = new AttributeValue();
        if (value.isTextual()) {
            attr.setS(value.textValue());
        } else if (value.isNumber()) {
            attr.setN(value.asText());
        } else if (value.isBoolean()) {
            attr.setBOOL(value.asBoolean());
        } else if (value.isArray()) {
            attr.setSS(IntStream.range(0,
                    value.size()).mapToObj(i -> value.get(i).asText())
                    .collect(Collectors.toList()));
        } else if (value.isObject()) {
            Map<String, AttributeValue> map = new HashMap<>(value.size());
            Iterator<Entry<String, JsonNode>> fields = value.fields();
            while (fields.hasNext()) {
                Entry<String, JsonNode> next = fields.next();
                map.put(next.getKey(), convertAttributeValue(next.getValue()));
            }
            attr.setM(map);
        } else if (!value.isNull()) {
            throw new IllegalStateException();
        }

        return attr;
    }

    @Override
    public List<Object> batchCreate(RequestContext context, List<User> users) {
        List<WriteRequest> collect = users.stream()
                .map(user -> new WriteRequest(new PutRequest(generatePutRequest(context.project, user.id, user.properties))))
                .collect(Collectors.toList());
        dynamoDBClient.batchWriteItem(new BatchWriteItemRequest().withRequestItems(ImmutableMap.of(context.project, collect)));
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> searchUsers(RequestContext context, List<String> columns, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, String offset) {
        QueryRequest scanRequest = new QueryRequest()
                .withTableName(tableConfig.getTableName());
        if (columns != null && !columns.isEmpty()) {
            scanRequest.withAttributesToGet(columns.stream().map(e -> "properties." + e).collect(Collectors.toList()));
        }
        scanRequest.withKeyConditions(ImmutableMap.of("project", new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(context.project))));

        final ImmutableMap.Builder<String, String> nameBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<String, AttributeValue> valueBuilder = ImmutableMap.builder();
        final char[] variable = {'a', 'a'};
        if (filterExpression != null) {
            DynamodbFilterQueryFormatter formatter = new DynamodbFilterQueryFormatter(variable, nameBuilder, valueBuilder);
            String expression = formatter.process(filterExpression, false);

            scanRequest.withFilterExpression(expression);
            ImmutableMap<String, String> names = nameBuilder.build();
            if (!names.isEmpty()) {
                scanRequest.withExpressionAttributeNames(names);
            }

            ImmutableMap<String, AttributeValue> values = valueBuilder.build();
            if (!values.isEmpty()) {
                scanRequest.withExpressionAttributeValues(values);
            }
        }

        scanRequest.withLimit(Math.toIntExact(limit));
        List<Map<String, AttributeValue>> scan = dynamoDBClient.query(scanRequest).getItems();

        Set<String> set = new HashSet<>();
        for (Map<String, AttributeValue> entry : scan) {
            for (Entry<String, AttributeValue> property : entry.get("properties").getM().entrySet()) {
                set.add(property.getKey());
            }
        }

        List<SchemaField> schemaFields = ImmutableList.<SchemaField>builder()
                .add(new SchemaField("id", STRING))
                .addAll(set.stream().map(e -> new SchemaField(e, STRING)).collect(Collectors.toList()))
                .build();

        List<List<Object>> result = new ArrayList<>();
        for (Map<String, AttributeValue> entry : scan) {
            Map<String, AttributeValue> properties = entry.get("properties").getM();

            ArrayList<Object> row = new ArrayList<>();
            row.add(getJsonValue(entry.get("id")));
            row.addAll(set.stream()
                    .map(name -> getJsonValue(properties.get(name)))
                    .collect(Collectors.toList()));
            result.add(row);
        }

        return CompletableFuture.completedFuture(new QueryResult(schemaFields, result));
    }

    @Override
    public void createSegment(RequestContext context, String name, String tableName, Expression filterExpression, List<EventFilter> eventFilter, Duration interval) {
        throw new RakamException("Unsupported", HttpResponseStatus.BAD_REQUEST);
    }

    @Override
    public List<SchemaField> getMetadata(RequestContext context) {
        return ImmutableList.of(new SchemaField("id", STRING));
    }

    @Override
    public CompletableFuture<User> getUser(RequestContext context, Object userId) {
        return CompletableFuture.supplyAsync(() -> {
            GetItemResult item = dynamoDBClient.getItem("users", ImmutableMap.of(
                    "project", new AttributeValue(context.project),
                    "id", new AttributeValue(userId.toString())
            ));
            Map<String, AttributeValue> attrs = item.getItem().get("properties").getM();
            ObjectNode obj = JsonHelper.jsonObject();
            for (Entry<String, AttributeValue> entry : attrs.entrySet()) {
                obj.set(entry.getKey(), getJsonValue(entry.getValue()));
            }
            return new User(userId, null, obj);
        });
    }

    private JsonNode getJsonValue(AttributeValue value) {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (value.getBOOL() != null) {
            return value.getBOOL() ? BooleanNode.TRUE : BooleanNode.FALSE;
        } else if (value.getS() != null) {
            return TextNode.valueOf(value.getS());
        } else if (value.getN() != null) {
            double v = Double.parseDouble(value.getN());
            return DoubleNode.valueOf(v);
        } else if (value.getL() != null) {
            ArrayNode arr = JsonHelper.jsonArray();
            for (AttributeValue attributeValue : value.getL()) {
                arr.add(getJsonValue(attributeValue));
            }
            return arr;
        } else if (value.getM() != null) {
            ObjectNode obj = JsonHelper.jsonObject();
            for (Entry<String, AttributeValue> attributeValue : value.getM().entrySet()) {
                obj.set(attributeValue.getKey(), getJsonValue(attributeValue.getValue()));
            }

            return obj;
        } else {
            if (!value.isNULL()) {
                throw new IllegalStateException();
            } else {
                return NullNode.getInstance();
            }
        }
    }

    private FieldType getType(AttributeValue value) {
        if (value.isBOOL()) {
            return FieldType.BOOLEAN;
        } else if (value.getS() != null) {
            return STRING;
        } else if (value.getN() != null) {
            return FieldType.DOUBLE;
        } else if (value.getL() != null) {
            return FieldType.ARRAY_STRING;
        } else if (value.getM() != null) {
            return FieldType.MAP_STRING;
        } else {
            if (!value.isNULL()) {
                throw new IllegalStateException();
            } else {
                return STRING;
            }
        }
    }

    @Override
    public void setUserProperties(String project, Object user, ObjectNode properties) {
        create(project, user, properties);
    }

    @Override
    public void setUserPropertiesOnce(String project, Object user, ObjectNode properties) {
        applyOperations(project,
                ImmutableList.of(new BatchUserOperations(user, null, properties, null, null, null)));
    }

    @Override
    public void applyOperations(String project, List<? extends ISingleUserBatchOperation> operations) {
        Map<String, String> nameMap = new HashMap<>();
        Map<String, AttributeValue> valueMap = new HashMap<>();
        StringBuilder setBuilder = null;
        StringBuilder addBuilder = null;
        StringBuilder unsetBuilder = null;
        List<UpdateItemRequest> setOnceBuilder = null;
        char nameCur = 'a';
        char valueCur = 'a';
        for (ISingleUserBatchOperation operation : operations) {
            for (Entry<String, Double> entry : operation.getIncrementProperties().entrySet()) {
                if (addBuilder == null) {
                    addBuilder = new StringBuilder();
                } else {
                    addBuilder.append(", ");
                }

                String name = new String(new char[]{'#', nameCur++});
                String value = new String(new char[]{':', valueCur++});
                addBuilder.append("properties." + name + " " + value);

                nameMap.put(name, entry.getKey());
                valueMap.put(value, new AttributeValue().withN(entry.getValue().toString()));
            }

            Iterator<Entry<String, JsonNode>> fields = operation.getSetProperties().fields();
            while (fields.hasNext()) {
                Entry<String, JsonNode> next = fields.next();
                if (setBuilder == null) {
                    setBuilder = new StringBuilder();
                } else {
                    setBuilder.append(", ");
                }

                String name = new String(new char[]{'#', nameCur++});
                String value = new String(new char[]{':', valueCur++});
                addBuilder.append("properties." + name + " = " + value);

                nameMap.put(name, next.getKey());
                valueMap.put(value, convertAttributeValue(next.getValue()));
            }

            Iterator<Entry<String, JsonNode>> setOncefields = operation.getSetPropertiesOnce().fields();
            while (setOncefields.hasNext()) {
                Entry<String, JsonNode> next = fields.next();

                if (setOnceBuilder == null) {
                    setOnceBuilder = new ArrayList<>();
                }

                setOnceBuilder.add(new UpdateItemRequest()
                        .withUpdateExpression("SET properties.#a = :a")
                        .withExpressionAttributeNames(ImmutableMap.of("#a", next.getKey()))
                        .withExpressionAttributeValues(ImmutableMap.of(":a", convertAttributeValue(next.getValue())))
                        .withTableName(tableConfig.getTableName())
                        .withKey(ImmutableMap.of("project", new AttributeValue(project), "id",
                                new AttributeValue(operation.getUser().toString())))
                );
            }

            for (String unsetProperty : operation.getUnsetProperties()) {
                if (unsetBuilder == null) {
                    unsetBuilder = new StringBuilder();
                } else {
                    unsetBuilder.append(" , ");
                }

                String name = new String(new char[]{'#', nameCur++});
                unsetBuilder.append("properties." + name);

                nameMap.put(name, unsetProperty);
            }

            StringBuilder query = new StringBuilder();
            if (setBuilder != null) {
                query.append("SET ").append(setBuilder);
            }
            if (unsetBuilder != null) {
                query.append("DELETE ").append(unsetBuilder);
            }
            if (addBuilder != null) {
                query.append("ADD ").append(addBuilder);
            }

            dynamoDBClient.updateItem(new UpdateItemRequest()
                    .withTableName(tableConfig.getTableName())
                    .withKey(ImmutableMap.of("project", new AttributeValue(project), "id",
                            new AttributeValue(operation.getUser().toString())))
                    .withUpdateExpression(query.toString()));

            if (setOnceBuilder != null) {
                setOnceBuilder.forEach(dynamoDBClient::updateItem);
            }
        }
    }

    @Override
    public void incrementProperty(String project, Object user, String property, double value) {

        applyOperations(project, ImmutableList.of(new BatchUserOperations(user,
                null, null, ImmutableMap.of(property, value), null, null)));
    }

    @Override
    public void dropProjectIfExists(String project) {
        dynamoDBClient.deleteItem(new DeleteItemRequest()
                .withKey(ImmutableMap.of("project", new AttributeValue(project))));
    }

    @Override
    public void unsetProperties(String project, Object user, List<String> properties) {
        applyOperations(project, ImmutableList.of(
                new BatchUserOperations(user, null, null, null, properties, null)));
    }
}
