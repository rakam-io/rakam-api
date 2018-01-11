package org.rakam.aws.dynamodb.user;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.facebook.presto.sql.tree.*;
import com.google.common.collect.ImmutableMap;
import org.rakam.util.RakamException;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

class DynamodbFilterQueryFormatter
        extends AstVisitor<String, Boolean> {
    private final char[] variable;
    private final ImmutableMap.Builder<String, String> nameBuilder;
    private final ImmutableMap.Builder<String, AttributeValue> valueBuilder;

    public DynamodbFilterQueryFormatter(char[] variable, ImmutableMap.Builder<String, String> nameBuilder, ImmutableMap.Builder<String, AttributeValue> valueBuilder) {
        this.variable = variable;
        this.nameBuilder = nameBuilder;
        this.valueBuilder = valueBuilder;
    }

    @Override
    protected String visitIsNullPredicate(IsNullPredicate node, Boolean unmangleNames) {
        if (!(node.getValue() instanceof Identifier)) {
            throw new IllegalArgumentException("inlined expressions are not supported");
        }

        return format("attribute_not_exists(%s)", process(node.getValue(), unmangleNames));
    }

    @Override
    protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean context) {
        return '(' + process(node.getLeft(), context) + ' ' + node.getType().name() + ' ' + process(node.getRight(), context) + ')';
    }

    @Override
    protected String visitNotExpression(NotExpression node, Boolean context) {
        return "(NOT " + process(node.getValue(), context) + ")";
    }

    @Override
    protected String visitNode(Node node, Boolean context) {
        throw new RakamException("The filter syntax is not supported", BAD_REQUEST);
    }

    @Override
    protected String visitIdentifier(Identifier node, Boolean context) {
        String variableName = "#" + variable[0]++;
        nameBuilder.put(variableName, node.getValue());
        return variableName;
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Boolean unmangleNames) {
        String variableName = ":" + variable[1]++;
        valueBuilder.put(variableName, new AttributeValue(node.getValue()));
        return variableName;
    }

    @Override
    protected String visitBooleanLiteral(BooleanLiteral node, Boolean unmangleNames) {
        String variableName = ":" + variable[1]++;
        valueBuilder.put(variableName, new AttributeValue().withBOOL(node.getValue()));
        return variableName;
    }

    @Override
    protected String visitLongLiteral(LongLiteral node, Boolean unmangleNames) {
        String variableName = ":" + variable[1]++;
        valueBuilder.put(variableName, new AttributeValue().withN(Long.toString(node.getValue())));
        return variableName;
    }

    @Override
    protected String visitDoubleLiteral(DoubleLiteral node, Boolean unmangleNames) {
        String variableName = ":" + variable[1]++;
        valueBuilder.put(variableName, new AttributeValue().withN(Double.toString(node.getValue())));
        return variableName;
    }

    @Override
    protected String visitDecimalLiteral(DecimalLiteral node, Boolean unmangleNames) {
        String variableName = ":" + variable[1]++;
        valueBuilder.put(variableName, new AttributeValue().withN(node.getValue()));
        return variableName;
    }

    @Override
    protected String visitComparisonExpression(ComparisonExpression node, Boolean unmangleNames) {
        return '(' + process(node.getLeft(), unmangleNames) + ' '
                + node.getType().getValue() + ' '
                + process(node.getRight(), unmangleNames) + ')';
    }

    @Override
    protected String visitIsNotNullPredicate(IsNotNullPredicate node, Boolean unmangleNames) {
        if (!(node.getValue() instanceof Identifier)) {
            throw new IllegalArgumentException("inlined expressions are not supported");
        }

        String variableName = "#" + variable[0]++;

        nameBuilder.put(variableName, ((Identifier) node.getValue()).getValue());

        return format("attribute_exists(%s)", variableName);
    }

    @Override
    protected String visitBetweenPredicate(BetweenPredicate node, Boolean unmangleNames) {
        if (!(node.getValue() instanceof Identifier)) {
            throw new IllegalArgumentException("inlined expressions are not supported");
        }

        if (!(node.getMin() instanceof Literal)) {
            throw new IllegalArgumentException("inlined expressions are not supported");
        }
        if (!(node.getMax() instanceof Literal)) {
            throw new IllegalArgumentException("inlined expressions are not supported");
        }

        String variableName = "#" + variable[0]++;
        nameBuilder.put(variableName, ((Identifier) node.getValue()).getValue());

        return "(#" + variableName + " BETWEEN " +
                process(node.getMin(), unmangleNames) + " AND " + process(node.getMax(), unmangleNames) + ")";
    }

    @Override
    protected String visitLikePredicate(LikePredicate node, Boolean unmangleNames) {
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
        } else if (pattern.endsWith("%s")) {
            throw new UnsupportedOperationException();
        } else if (pattern.startsWith("%s")) {
            value = pattern.substring(1, pattern.length() - 1);
            template = "begins_with(%s, %s)";
        } else {
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
