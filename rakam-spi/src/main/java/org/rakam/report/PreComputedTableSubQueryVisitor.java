package org.rakam.report;

import com.facebook.presto.sql.tree.*;
import org.rakam.util.ValidationUtil;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.OR;

public class PreComputedTableSubQueryVisitor extends AstVisitor<String, Boolean> {

    private final Function<String, Optional<String>> columnNameMapper;

    public PreComputedTableSubQueryVisitor(Function<String, Optional<String>> columnNameMapper) {
        this.columnNameMapper = columnNameMapper;
    }

    @Override
    protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean negate) {
        LogicalBinaryExpression.Type type = node.getType();
        if (type == AND) {
            // TODO find a better way
            // Optimization for the case when one hand or binary expression is IS NOT NULL predicate
            if (node.getRight() instanceof IsNotNullPredicate && !(node.getLeft() instanceof IsNotNullPredicate) ||
                    node.getLeft() instanceof IsNotNullPredicate && !(node.getRight() instanceof IsNotNullPredicate)) {
                Expression isNotNull = node.getRight() instanceof IsNotNullPredicate ? node.getRight() : node.getLeft();
                Expression setExpression = isNotNull == node.getRight() ? node.getLeft() : node.getRight();

                String excludeQuery = process(new IsNullPredicate(((IsNotNullPredicate) isNotNull).getValue()), negate);
                return "SELECT l.date, l.dimension, l._user_set FROM (" + process(setExpression, negate) + ") l LEFT JOIN (" + excludeQuery + ") r ON (r.date = l.date AND l.dimension = r.dimension) WHERE r.date IS NULL";
            }
            String right = process(node.getRight(), negate);
            String left = process(node.getLeft(), negate);

            // TODO: use INTERSECT when it's implemented in Presto.
            return "SELECT l.date, l.dimension, l._user_set FROM (" + left + ") l JOIN (" + right + ") r ON (r.date = l.date)";
        } else if (type == OR) {
            return "SELECT date, dimension, _user_set FROM (" + process(node.getLeft(), negate) +
                    " UNION ALL " + process(node.getRight(), negate) + ")";
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    protected String visitNotExpression(NotExpression node, Boolean negate) {
        return process(node.getValue(), !negate);
    }

    @Override
    protected String visitComparisonExpression(ComparisonExpression node, Boolean negate) {
        String left = process(node.getLeft(), negate);
        String right = process(node.getRight(), negate);
        String predicate = node.getType().getValue() + " " + right;
        if (negate) {
            predicate = String.format("not(%s)", predicate);
        }
        return "SELECT date, dimension, _user_set FROM " + left + " WHERE dimension " + predicate;
    }

    @Override
    protected String visitLongLiteral(LongLiteral node, Boolean negate) {
        return Long.toString(node.getValue());
    }

    @Override
    protected String visitLikePredicate(LikePredicate node, Boolean negate) {
        return "SELECT date, dimension, _user_set FROM " + process(node.getValue(), negate) +
                " WHERE dimension LIKE " + process(node.getPattern(), negate);
    }

    @Override
    protected String visitIsNotNullPredicate(IsNotNullPredicate node, Boolean negate) {
        if (negate) {
            return visitIsNullPredicate(new IsNullPredicate(node.getValue()), !negate);
        }
        String column = process(node.getValue(), negate);
        return "SELECT date, dimension, _user_set FROM " + column + " WHERE dimension is not null";
    }

    @Override
    protected String visitIsNullPredicate(IsNullPredicate node, Boolean negate) {
        if (negate) {
            return visitIsNullPredicate(new IsNullPredicate(node.getValue()), !negate);
        }
        return "SELECT date, dimension, _user_set FROM " + process(node.getValue(), negate) + " WHERE dimension is null";
    }

    @Override
    protected String visitInPredicate(InPredicate node, Boolean negate) {
        String predicate = "IN " + process(node.getValue(), null) + " " + process(node, negate) + " ) ";
        if (negate) {
            predicate = String.format("NOT ", predicate);
        }
        return "SELECT date, dimension, _user_set FROM " + process(node.getValue(), negate) + " WHERE dimension " + predicate;
    }

    @Override
    protected String visitBetweenPredicate(BetweenPredicate node, Boolean negate) {
        String predicate = "BETWEEN " + process(node.getMin(), null) + " " + process(node, negate) + " ) ";
        if (negate) {
            predicate = String.format("not(%s)", predicate);
        }
        return "SELECT date, dimension, _user_set FROM " + process(node.getValue(), negate) + " WHERE dimension " + predicate;
    }

    @Override
    protected String visitDoubleLiteral(DoubleLiteral node, Boolean negate) {
        return Double.toString(node.getValue());
    }

    @Override
    protected String visitGenericLiteral(GenericLiteral node, Boolean negate) {
        return node.getType() + " '" + node.getValue() + "'";
    }

    @Override
    protected String visitTimeLiteral(TimeLiteral node, Boolean negate) {
        return "TIME '" + node.getValue() + "'";
    }

    @Override
    protected String visitTimestampLiteral(TimestampLiteral node, Boolean negate) {
        return "TIMESTAMP '" + node.getValue() + "'";
    }

    @Override
    protected String visitNullLiteral(NullLiteral node, Boolean negate) {
        return "null";
    }

    @Override
    protected String visitIntervalLiteral(IntervalLiteral node, Boolean negate) {
        String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "- " : "";
        StringBuilder builder = new StringBuilder()
                .append("INTERVAL ")
                .append(sign)
                .append(" '").append(node.getValue()).append("' ")
                .append(node.getStartField());

        if (node.getEndField().isPresent()) {
            builder.append(" TO ").append(node.getEndField().get());
        }
        return builder.toString();
    }

    @Override
    protected String visitBooleanLiteral(BooleanLiteral node, Boolean negate) {
        return String.valueOf(node.getValue());
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Boolean negate) {
        return "'" + node.getValue().replace("'", "''") + "'";
    }

    @Override
    protected String visitIdentifier(Identifier node, Boolean context) {
        String tableColumn = ValidationUtil
                .checkTableColumn(node.getValue(), "reference in filter", '"');

        Optional<String> preComputedTable = columnNameMapper.apply(tableColumn);
        if (preComputedTable.isPresent()) {
            return preComputedTable.get();
        }

        throw new UnsupportedOperationException();
    }

    @Override
    protected String visitNode(Node node, Boolean negate) {
        throw new UnsupportedOperationException();
    }
}
