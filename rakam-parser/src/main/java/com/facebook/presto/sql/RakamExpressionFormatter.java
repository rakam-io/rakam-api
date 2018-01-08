package com.facebook.presto.sql;

import com.facebook.presto.sql.ExpressionFormatter.Formatter;
import com.facebook.presto.sql.tree.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static com.facebook.presto.sql.RakamSqlFormatter.formatSql;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class RakamExpressionFormatter
        extends Formatter {

    private final Function<QualifiedName, String> tableNameMapper;
    private final Optional<Function<String, String>> columnNameMapper;
    private final char escape;
    private final List<String> queryWithTables;

    public RakamExpressionFormatter(
            Function<QualifiedName, String> tableNameMapper,
            Optional<Function<String, String>> columnNameMapper,
            char escape) {
        this(tableNameMapper, columnNameMapper, null, escape);
    }

    RakamExpressionFormatter(
            Function<QualifiedName, String> tableNameMapper,
            Optional<Function<String, String>> columnNameMapper,
            List<String> queryWithTables,
            char escape) {
        super(Optional.empty());
        this.tableNameMapper = tableNameMapper;
        this.columnNameMapper = columnNameMapper;
        this.queryWithTables = queryWithTables;
        this.escape = escape;
    }

    private static String formatQualifiedName(QualifiedName name, char escape) {
        List<String> parts = new ArrayList<>();
        for (String part : name.getParts()) {
            parts.add(formatIdentifier(part, escape));
        }
        return Joiner.on('.').join(parts);
    }

    public static String formatIdentifier(String s, char escape) {
        // TODO: handle escaping properly
        return escape + s + escape;
    }

    static String formatStringLiteral(String s) {
        // ALERT: MYSQL DOES NOT SUPPORT THIS SCHEME
        s = s.replace("'", "''");
//        if (isAsciiPrintable(s)) {
        return "'" + s + "'";
//        }

//        StringBuilder builder = new StringBuilder();
//        builder.append("U&'");
//        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
//        while (iterator.hasNext()) {
//            int codePoint = iterator.nextInt();
//            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: " + s);
//            if (isAsciiPrintable(codePoint)) {
//                char ch = (char) codePoint;
//                if (ch == '\\') {
//                    builder.append(ch);
//                }
//                builder.append(ch);
//            }
//            else if (codePoint <= 0xFFFF) {
//                builder.append('\\');
//                builder.append(String.format("%04X", codePoint));
//            }
//            else {
//                builder.append("\\+");
//                builder.append(String.format("%06X", codePoint));
//            }
//        }
//        builder.append("'");
//        return builder.toString();
    }

    static String formatOrderBy(OrderBy orderBy, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, List<String> queryWithTables, char escapeIdentifier) {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier);
    }

    static String formatSortItems(List<SortItem> sortItems, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, List<String> queryWithTables, char escapeIdentifier) {
        return Joiner.on(", ").join(sortItems.stream()
                .map(sortItemFormatterFunction(tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                .iterator());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, List<String> queryWithTables, char escapeIdentifier) {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                Set<Expression> columns = ImmutableSet.copyOf(((SimpleGroupBy) groupingElement).getColumnExpressions());
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier);
                } else {
                    result = formatGroupingSet(columns, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier);
                }
            } else if (groupingElement instanceof GroupingSets) {
                result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                        ((GroupingSets) groupingElement).getSets().stream()
                                .map(RakamExpressionFormatter::formatGroupingSet)
                                .iterator()));
            } else if (groupingElement instanceof Cube) {
                result = format("CUBE %s", formatGroupingSet(((Cube) groupingElement).getColumns()));
            } else if (groupingElement instanceof Rollup) {
                result = format("ROLLUP %s", formatGroupingSet(((Rollup) groupingElement).getColumns()));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    private static boolean isAsciiPrintable(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!isAsciiPrintable(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiPrintable(int codePoint) {
        if (codePoint >= 0x7F || codePoint < 0x20) {
            return false;
        }
        return true;
    }

    private static String formatGroupingSet(Set<Expression> groupingSet, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, List<String> queryWithTables, char escapeIdentifier) {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(e -> formatExpression(e, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                .iterator()));
    }

    private static String formatGroupingSet(List<QualifiedName> groupingSet) {
        return format("(%s)", Joiner.on(", ").join(groupingSet));
    }

    private static Function<SortItem, String> sortItemFormatterFunction(Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, List<String> queryWithTables, char escapeIdentifier) {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));

            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }

    @Override
    protected String visitNode(Node node, Void context) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String visitRow(Row node, Void context) {
        return "ROW (" + Joiner.on(", ").join(node.getItems().stream()
                .map((child) -> process(child, context))
                .collect(toList())) + ")";
    }

    @Override
    protected String visitExpression(Expression node, Void context) {
        throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
    }

    @Override
    protected String visitAtTimeZone(AtTimeZone node, Void context) {
        return new StringBuilder()
                .append(process(node.getValue(), context))
                .append(" AT TIME ZONE ")
                .append(process(node.getTimeZone(), context)).toString();
    }

    @Override
    protected String visitCurrentTime(CurrentTime node, Void context) {
        StringBuilder builder = new StringBuilder();

        builder.append(node.getType().getName());

        if (node.getPrecision() != null) {
            builder.append('(')
                    .append(node.getPrecision())
                    .append(')');
        }

        return builder.toString();
    }

    @Override
    protected String visitExtract(Extract node, Void context) {
        return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
    }

    @Override
    protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
        return String.valueOf(node.getValue());
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Void context) {
        return formatStringLiteral(node.getValue());
    }

    @Override
    protected String visitCharLiteral(CharLiteral node, Void context) {
        return "CHAR " + formatStringLiteral(node.getValue());
    }

    @Override
    protected String visitBinaryLiteral(BinaryLiteral node, Void context) {
        return "X'" + node.toHexString() + "'";
    }

    @Override
    protected String visitParameter(Parameter node, Void context) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String visitArrayConstructor(ArrayConstructor node, Void context) {
        ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
        for (Expression value : node.getValues()) {
            valueStrings.add(formatSql(value, tableNameMapper, columnNameMapper.orElse(null), queryWithTables, escape));
        }
        return "array[" + Joiner.on(",").join(valueStrings.build()) + "]";
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
        return formatSql(node.getBase(), tableNameMapper, columnNameMapper.orElse(null), queryWithTables, escape) + "[" + formatSql(node.getIndex(), tableNameMapper, columnNameMapper.orElse(null), queryWithTables, escape) + "]";
    }

    @Override
    protected String visitLongLiteral(LongLiteral node, Void context) {
        return Long.toString(node.getValue());
    }

    @Override
    protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
        return Double.toString(node.getValue());
    }

    @Override
    protected String visitDecimalLiteral(DecimalLiteral node, Void context) {
        return "DECIMAL '" + node.getValue() + "'";
    }

    @Override
    protected String visitGenericLiteral(GenericLiteral node, Void context) {
        return node.getType() + " " + formatStringLiteral(node.getValue());
    }

    @Override
    protected String visitTimeLiteral(TimeLiteral node, Void context) {
        return "TIME '" + node.getValue() + "'";
    }

    @Override
    protected String visitTimestampLiteral(TimestampLiteral node, Void context) {
        return "TIMESTAMP '" + node.getValue() + "'";
    }

    @Override
    protected String visitNullLiteral(NullLiteral node, Void context) {
        return "null";
    }

    @Override
    protected String visitIntervalLiteral(IntervalLiteral node, Void context) {
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
    protected String visitSubqueryExpression(SubqueryExpression node, Void context) {
        return "(" + formatSql(node.getQuery(), tableNameMapper, columnNameMapper.orElse(null), queryWithTables, escape) + ")";
    }

    @Override
    protected String visitExists(ExistsPredicate node, Void context) {
        return "(EXISTS " + formatSql(node.getSubquery(), tableNameMapper, columnNameMapper.orElse(null), queryWithTables, escape) + ")";
    }

    @Override
    protected String visitIdentifier(Identifier node, Void context) {
        if (columnNameMapper.isPresent()) {
            return columnNameMapper.get().apply(node.getValue());
        }
        return formatIdentifier(node.getValue(), escape);
    }

    @Override
    protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context) {
        return formatIdentifier(node.getName().getValue(), escape);
    }

    @Override
    protected String visitSymbolReference(SymbolReference node, Void context) {
        return formatIdentifier(node.getName(), escape);
    }

    @Override
    protected String visitDereferenceExpression(DereferenceExpression node, Void context) {
        String baseString = process(node.getBase(), context);
        return baseString + "." + formatIdentifier(node.getField().getValue(), escape);
    }

    @Override
    public String visitFieldReference(FieldReference node, Void context) {
        // add colon so this won't parse
        return ":input(" + node.getFieldIndex() + ")";
    }

    @Override
    protected String visitFunctionCall(FunctionCall node, Void context) {
        StringBuilder builder = new StringBuilder();

        String arguments = joinExpressions(node.getArguments());
        if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
            arguments = "*";
        }
        if (node.isDistinct()) {
            arguments = "DISTINCT " + arguments;
        }

        String name;
        if (node.getName().getParts().size() > 1) {
            name = formatQualifiedName(node.getName(), escape);
        } else {
            // for Mysql - > SELECT `count`(1) doesn't work
            name = node.getName().getSuffix();
        }

        builder.append(name).append('(').append(arguments).append(')');

        if (node.getFilter().isPresent()) {
            builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), context));
        }

        if (node.getWindow().isPresent()) {
            builder.append(" OVER ").append(visitWindow(node.getWindow().get(), context));
        }

        return builder.toString();
    }

    @Override
    protected String visitLambdaExpression(LambdaExpression node, Void context) {
        StringBuilder builder = new StringBuilder();

        builder.append('(');
        Joiner.on(", ").appendTo(builder, node.getArguments());
        builder.append(") -> ");
        builder.append(process(node.getBody(), context));
        return builder.toString();
    }

    @Override
    protected String visitBindExpression(BindExpression node, Void context) {
        StringBuilder builder = new StringBuilder();

        builder.append("\"$INTERNAL$BIND\"(");
        for (Expression value : node.getValues()) {
            builder.append(process(value, context) + ", ");
        }
        builder.append(process(node.getFunction(), context) + ")");
        return builder.toString();
    }

    @Override
    protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
        return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight());
    }

    @Override
    protected String visitNotExpression(NotExpression node, Void context) {
        return "(NOT " + process(node.getValue(), context) + ")";
    }

    @Override
    protected String visitComparisonExpression(ComparisonExpression node, Void context) {
        return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
    }

    @Override
    protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
        return "(" + process(node.getValue(), context) + " IS NULL)";
    }

    @Override
    protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
        return "(" + process(node.getValue(), context) + " IS NOT NULL)";
    }

    @Override
    protected String visitNullIfExpression(NullIfExpression node, Void context) {
        return "NULLIF(" + process(node.getFirst(), context) + ", " + process(node.getSecond(), context) + ')';
    }

    @Override
    protected String visitIfExpression(IfExpression node, Void context) {
        StringBuilder builder = new StringBuilder();
        builder.append("IF(")
                .append(process(node.getCondition(), context))
                .append(", ")
                .append(process(node.getTrueValue(), context));
        if (node.getFalseValue().isPresent()) {
            builder.append(", ")
                    .append(process(node.getFalseValue().get(), context));
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    protected String visitTryExpression(TryExpression node, Void context) {
        return "TRY(" + process(node.getInnerExpression(), context) + ")";
    }

    @Override
    protected String visitCoalesceExpression(CoalesceExpression node, Void context) {
        return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
    }

    @Override
    protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
        String value = process(node.getValue(), context);

        switch (node.getSign()) {
            case MINUS:
                // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
                String separator = value.startsWith("-") ? " " : "";
                return "-" + separator + value;
            case PLUS:
                return "+" + value;
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
        }
    }

    @Override
    protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
        return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
    }

    @Override
    protected String visitLikePredicate(LikePredicate node, Void context) {
        StringBuilder builder = new StringBuilder();

        builder.append('(')
                .append(process(node.getValue(), context))
                .append(" LIKE ")
                .append(process(node.getPattern(), context));

        if (node.getEscape() != null) {
            builder.append(" ESCAPE ")
                    .append(process(node.getEscape(), context));
        }

        builder.append(')');

        return builder.toString();
    }

    @Override
    protected String visitAllColumns(AllColumns node, Void context) {
        if (node.getPrefix().isPresent()) {
            return node.getPrefix().get() + ".*";
        }

        return "*";
    }

    @Override
    public String visitCast(Cast node, Void context) {
        return (node.isSafe() ? "TRY_CAST" : "CAST") +
                "(" + process(node.getExpression(), context) + " AS " + node.getType() + ")";
    }

    @Override
    protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        parts.add("CASE");
        for (WhenClause whenClause : node.getWhenClauses()) {
            parts.add(process(whenClause, context));
        }

        node.getDefaultValue()
                .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

        parts.add("END");

        return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
        ImmutableList.Builder<String> parts = ImmutableList.builder();

        parts.add("CASE")
                .add(process(node.getOperand(), context));

        for (WhenClause whenClause : node.getWhenClauses()) {
            parts.add(process(whenClause, context));
        }

        node.getDefaultValue()
                .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

        parts.add("END");

        return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    protected String visitWhenClause(WhenClause node, Void context) {
        return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
    }

    @Override
    protected String visitBetweenPredicate(BetweenPredicate node, Void context) {
        return "(" + process(node.getValue(), context) + " BETWEEN " +
                process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
    }

    @Override
    protected String visitInPredicate(InPredicate node, Void context) {
        return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
    }

    @Override
    protected String visitInListExpression(InListExpression node, Void context) {
        return "(" + joinExpressions(node.getValues()) + ")";
    }

    private String visitFilter(Expression node, Void context) {
        return "(WHERE " + process(node, context) + ')';
    }

    @Override
    public String visitWindow(Window node, Void context) {
        List<String> parts = new ArrayList<>();

        if (!node.getPartitionBy().isEmpty()) {
            parts.add("PARTITION BY " + joinExpressions(node.getPartitionBy()));
        }
        if (node.getOrderBy().isPresent()) {
            parts.add(formatOrderBy(node.getOrderBy().get(), tableNameMapper, columnNameMapper, queryWithTables, escape));
        }
        if (node.getFrame().isPresent()) {
            parts.add(process(node.getFrame().get(), context));
        }

        return '(' + Joiner.on(' ').join(parts) + ')';
    }

    @Override
    public String visitWindowFrame(WindowFrame node, Void context) {
        StringBuilder builder = new StringBuilder();

        builder.append(node.getType().toString()).append(' ');

        if (node.getEnd().isPresent()) {
            builder.append("BETWEEN ")
                    .append(process(node.getStart(), context))
                    .append(" AND ")
                    .append(process(node.getEnd().get(), context));
        } else {
            builder.append(process(node.getStart(), context));
        }

        return builder.toString();
    }

    @Override
    public String visitFrameBound(FrameBound node, Void context) {
        switch (node.getType()) {
            case UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING";
            case PRECEDING:
                return process(node.getValue().get(), context) + " PRECEDING";
            case CURRENT_ROW:
                return "CURRENT ROW";
            case FOLLOWING:
                return process(node.getValue().get(), context) + " FOLLOWING";
            case UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING";
        }
        throw new IllegalArgumentException("unhandled type: " + node.getType());
    }

    @Override
    protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context) {
        return new StringBuilder()
                .append("(")
                .append(process(node.getValue(), context))
                .append(' ')
                .append(node.getComparisonType().getValue())
                .append(' ')
                .append(node.getQuantifier().toString())
                .append(' ')
                .append(process(node.getSubquery(), context))
                .append(")")
                .toString();
    }

    public String visitGroupingOperation(GroupingOperation node, Void context) {
        return "GROUPING (" + joinExpressions(node.getGroupingColumns()) + ")";
    }

    private String formatBinaryExpression(String operator, Expression left, Expression right) {
        return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
    }

    private String joinExpressions(List<Expression> expressions) {
        return Joiner.on(", ").join(expressions.stream()
                .map((e) -> process(e, null))
                .iterator());
    }
}
