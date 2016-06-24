package org.rakam.aws.dynamodb.user;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.rakam.aws.AWSConfig;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.User;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryResult;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.annotation.PostConstruct;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.STRING;

public class DynamodbUserStorage
        implements UserStorage
{

    private final AmazonDynamoDBClient dynamoDBClient;
    private static final List<KeySchemaElement> PROJECT_KEYSCHEMA = ImmutableList.of(
            new KeySchemaElement().withKeyType(HASH).withAttributeName("project"),
            new KeySchemaElement().withKeyType(RANGE).withAttributeName("id")
    );
    private static final List<AttributeDefinition> ATTRIBUTES = ImmutableList.of(
            new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("project").withAttributeType(ScalarAttributeType.S)
    );
    private final DynamodbUserConfig tableConfig;

    @Inject
    public DynamodbUserStorage(AWSConfig config, DynamodbUserConfig tableConfig)
    {
        dynamoDBClient = new AmazonDynamoDBClient(config.getCredentials());
        if (config.getDynamodbEndpoint() != null) {
            dynamoDBClient.setEndpoint(config.getDynamodbEndpoint());
        }
        this.tableConfig = tableConfig;
    }

    @PostConstruct
    public void setup()
    {
        try {
            DescribeTableResult result = dynamoDBClient.describeTable(tableConfig.getTableName());

            if (!result.getTable().getKeySchema().equals(PROJECT_KEYSCHEMA)) {
                throw new IllegalStateException();
            }
            if (!result.getTable().getAttributeDefinitions().equals(ATTRIBUTES)) {
                throw new IllegalStateException();
            }
        }
        catch (ResourceNotFoundException e) {
            dynamoDBClient.createTable(new CreateTableRequest().withTableName(tableConfig.getTableName())
                    .withKeySchema(PROJECT_KEYSCHEMA)
                    .withAttributeDefinitions(ATTRIBUTES)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(3L)
                            .withWriteCapacityUnits(3L)));
        }
    }

    @Override
    public Object create(String project, Object id, ObjectNode properties)
    {
        dynamoDBClient.putItem(new PutItemRequest().withTableName(tableConfig.getTableName())
                .withItem(generatePutRequest(project, id, properties)));
        return id;
    }

    private Map<String, AttributeValue> generatePutRequest(String project, Object id, ObjectNode properties)
    {
        Map<String, AttributeValue> builder = new HashMap<>();
        builder.put("project", new AttributeValue(project));
        builder.put("id", new AttributeValue(id.toString()));

        Map<String, AttributeValue> props = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> next = fields.next();

            props.put(next.getKey(), convertAttributeValue(next.getValue()));
        }

        builder.put("properties", new AttributeValue().withM(props));

        return builder;
    }

    private AttributeValue convertAttributeValue(JsonNode value)
    {
        AttributeValue attr = new AttributeValue();
        if (value.isTextual()) {
            attr.setS(value.textValue());
        }
        else if (value.isNumber()) {
            attr.setN(value.asText());
        }
        else if (value.isBoolean()) {
            attr.setBOOL(value.asBoolean());
        }
        else if (value.isArray()) {
            attr.setSS(IntStream.range(0,
                    value.size()).mapToObj(i -> value.get(i).asText())
                    .collect(Collectors.toList()));
        }
        else if (value.isObject()) {
            Map<String, AttributeValue> map = new HashMap<>(value.size());
            Iterator<Map.Entry<String, JsonNode>> fields = value.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> next = fields.next();
                map.put(next.getKey(), convertAttributeValue(next.getValue()));
            }
            attr.setM(map);
        }
        else if (!value.isNull()) {
            throw new IllegalStateException();
        }

        return attr;
    }

    @Override
    public List<Object> batchCreate(String project, List<User> users)
    {
        List<WriteRequest> collect = users.stream()
                .map(user -> new WriteRequest(new PutRequest(generatePutRequest(project, user.id, user.properties))))
                .collect(Collectors.toList());
        dynamoDBClient.batchWriteItem(new BatchWriteItemRequest().withRequestItems(ImmutableMap.of(project, collect)));
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> searchUsers(String project, List<String> columns, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, String offset)
    {
        QueryRequest scanRequest = new QueryRequest()
                .withTableName(tableConfig.getTableName());
        if (columns != null && !columns.isEmpty()) {
            scanRequest.withAttributesToGet(columns.stream().map(e -> "properties." + e).collect(Collectors.toList()));
        }
        scanRequest.withKeyConditions(ImmutableMap.of("project", new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(project))));

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
            for (Map.Entry<String, AttributeValue> property : entry.get("properties").getM().entrySet()) {
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
    public void createSegment(String project, String name, String tableName, Expression filterExpression, List<EventFilter> eventFilter, Duration interval)
    {
        //
    }

    @Override
    public List<SchemaField> getMetadata(String project)
    {
        return ImmutableList.of(new SchemaField("id", STRING));
    }

    @Override
    public CompletableFuture<User> getUser(String project, Object userId)
    {
        return CompletableFuture.supplyAsync(() -> {
            GetItemResult item = dynamoDBClient.getItem("users", ImmutableMap.of(
                    "project", new AttributeValue(project),
                    "id", new AttributeValue(userId.toString())
            ));
            Map<String, AttributeValue> attrs = item.getItem().get("properties").getM();
            ObjectNode obj = JsonHelper.jsonObject();
            for (Map.Entry<String, AttributeValue> entry : attrs.entrySet()) {
                obj.set(entry.getKey(), getJsonValue(entry.getValue()));
            }
            return new User(userId, null, obj);
        });
    }

    private JsonNode getJsonValue(AttributeValue value)
    {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (value.getBOOL() != null) {
            return value.getBOOL() ? BooleanNode.TRUE : BooleanNode.FALSE;
        }
        else if (value.getS() != null) {
            return TextNode.valueOf(value.getS());
        }
        else if (value.getN() != null) {
            double v = Double.parseDouble(value.getN());
            return DoubleNode.valueOf(v);
        }
        else if (value.getL() != null) {
            ArrayNode arr = JsonHelper.jsonArray();
            for (AttributeValue attributeValue : value.getL()) {
                arr.add(getJsonValue(attributeValue));
            }
            return arr;
        }
        else if (value.getM() != null) {
            ObjectNode obj = JsonHelper.jsonObject();
            for (Map.Entry<String, AttributeValue> attributeValue : value.getM().entrySet()) {
                obj.set(attributeValue.getKey(), getJsonValue(attributeValue.getValue()));
            }

            return obj;
        }
        else {
            if (!value.isNULL()) {
                throw new IllegalStateException();
            }
            else {
                return NullNode.getInstance();
            }
        }
    }

    private FieldType getType(AttributeValue value)
    {
        if (value.isBOOL()) {
            return FieldType.BOOLEAN;
        }
        else if (value.getS() != null) {
            return STRING;
        }
        else if (value.getN() != null) {
            return FieldType.DOUBLE;
        }
        else if (value.getL() != null) {
            return FieldType.ARRAY_STRING;
        }
        else if (value.getM() != null) {
            return FieldType.MAP_STRING;
        }
        else {
            if (!value.isNULL()) {
                throw new IllegalStateException();
            }
            else {
                return STRING;
            }
        }
    }

    @Override
    public void setUserProperty(String project, Object user, ObjectNode properties)
    {
        create(project, user, properties);
    }

    @Override
    public void setUserPropertyOnce(String project, Object user, ObjectNode properties)
    {

    }

    @Override
    public void incrementProperty(String project, Object user, String property, double value)
    {

    }

    @Override
    public void dropProjectIfExists(String project)
    {

    }

    @Override
    public void unsetProperties(String project, Object user, List<String> properties)
    {

    }

    private static class DynamodbFilterQueryFormatter
            extends AstVisitor<String, Boolean>
    {
        private final char[] variable;
        private final ImmutableMap.Builder<String, String> nameBuilder;
        private final ImmutableMap.Builder<String, AttributeValue> valueBuilder;

        public DynamodbFilterQueryFormatter(char[] variable, ImmutableMap.Builder<String, String> nameBuilder, ImmutableMap.Builder<String, AttributeValue> valueBuilder)
        {
            this.variable = variable;
            this.nameBuilder = nameBuilder;
            this.valueBuilder = valueBuilder;
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Boolean unmangleNames)
        {
            if (!(node.getValue() instanceof QualifiedNameReference)) {
                throw new IllegalArgumentException("inlined expressions are not supported");
            }

            return format("attribute_not_exists(%s)", process(node.getValue(), unmangleNames));
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean context)
        {
            return '(' + process(node.getLeft(), context) + ' ' + node.getType().name() + ' ' + process(node.getRight(), context) + ')';
        }

        @Override
        protected String visitNotExpression(NotExpression node, Boolean context)
        {
            return "(NOT " + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitNode(Node node, Boolean context)
        {
            throw new RakamException("The filter syntax is not supported", BAD_REQUEST);
        }

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Boolean unmangleNames)
        {
            String variableName = "#" + variable[0]++;
            nameBuilder.put(variableName, node.getName().toString());
            return variableName;
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Boolean unmangleNames)
        {
            String variableName = ":" + variable[1]++;
            valueBuilder.put(variableName, new AttributeValue(node.getValue()));
            return variableName;
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Boolean unmangleNames)
        {
            String variableName = ":" + variable[1]++;
            valueBuilder.put(variableName, new AttributeValue().withBOOL(node.getValue()));
            return variableName;
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Boolean unmangleNames)
        {
            String variableName = ":" + variable[1]++;
            valueBuilder.put(variableName, new AttributeValue().withN(Long.toString(node.getValue())));
            return variableName;
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Boolean unmangleNames)
        {
            String variableName = ":" + variable[1]++;
            valueBuilder.put(variableName, new AttributeValue().withN(Double.toString(node.getValue())));
            return variableName;
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Boolean unmangleNames)
        {
            String variableName = ":" + variable[1]++;
            valueBuilder.put(variableName, new AttributeValue().withN(node.getValue()));
            return variableName;
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Boolean unmangleNames)
        {
            return '(' + process(node.getLeft(), unmangleNames) + ' '
                    + node.getType().getValue() + ' '
                    + process(node.getRight(), unmangleNames) + ')';
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Boolean unmangleNames)
        {
            if (!(node.getValue() instanceof QualifiedNameReference)) {
                throw new IllegalArgumentException("inlined expressions are not supported");
            }

            String variableName = "#" + variable[0]++;

            nameBuilder.put(variableName, ((QualifiedNameReference) node.getValue()).getName().toString());

            return format("attribute_exists(%s)", variableName);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Boolean unmangleNames)
        {
            if (!(node.getValue() instanceof QualifiedNameReference)) {
                throw new IllegalArgumentException("inlined expressions are not supported");
            }

            if (!(node.getMin() instanceof Literal)) {
                throw new IllegalArgumentException("inlined expressions are not supported");
            }
            if (!(node.getMax() instanceof Literal)) {
                throw new IllegalArgumentException("inlined expressions are not supported");
            }

            String variableName = "#" + variable[0]++;
            nameBuilder.put(variableName, ((QualifiedNameReference) node.getValue()).getName().toString());

            return "(#" + variableName + " BETWEEN " +
                    process(node.getMin(), unmangleNames) + " AND " + process(node.getMax(), unmangleNames) + ")";
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Boolean unmangleNames)
        {
            StringBuilder builder = new StringBuilder();

            if (!(node.getPattern() instanceof StringLiteral)) {
                throw new RakamException("LIKE clause can only have string pattern", BAD_REQUEST);
            }

            String template;
            String value;
            String pattern = ((StringLiteral) node.getPattern()).getValue();
            if (pattern.endsWith("%") && pattern.startsWith("%")) {
                template = "contains(%s, %s)";
                value = pattern.substring(1, pattern.length() - 2);
            }
            else if (pattern.endsWith("%s")) {
                throw new UnsupportedOperationException();
            }
            else if (pattern.startsWith("%s")) {
                value = pattern.substring(1, pattern.length() - 1);
                template = "begins_with(%s, %s)";
            }
            else {
                value = pattern;
                template = "%s = %s";
            }

            if (node.getEscape() != null) {
                throw new RakamException("ESCAPE is not supported LIKE statement", BAD_REQUEST);
            }

            builder.append(format(template,
                    process(node.getValue(), unmangleNames),
                    visitStringLiteral(new StringLiteral(value), false)));

            return builder.toString();
        }
    }
}
