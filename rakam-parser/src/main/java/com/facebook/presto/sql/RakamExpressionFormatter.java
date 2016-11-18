package com.facebook.presto.sql;

import com.facebook.presto.sql.ExpressionFormatter.Formatter;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
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

public class RakamExpressionFormatter
        extends Formatter
{

    private final Function<QualifiedName, String> tableNameMapper;
    private final Optional<Function<QualifiedName, String>> columnNameMapper;
    private final char escape;
    private final List<String> queryWithTables;

    public RakamExpressionFormatter(
            Function<QualifiedName, String> tableNameMapper,
            Optional<Function<QualifiedName, String>> columnNameMapper,
            char escape)
    {
        this(tableNameMapper, columnNameMapper, null, escape);
    }

    RakamExpressionFormatter(
            Function<QualifiedName, String> tableNameMapper,
            Optional<Function<QualifiedName, String>> columnNameMapper,
            List<String> queryWithTables,
            char escape)
    {
        super(Optional.empty());
        this.tableNameMapper = tableNameMapper;
        this.columnNameMapper = columnNameMapper;
        this.queryWithTables = queryWithTables;
        this.escape = escape;
    }

    @Override
    protected String visitFunctionCall(FunctionCall node, Boolean unmangleNames)
    {
        StringBuilder builder = new StringBuilder();

        String arguments = joinExpressions(node.getArguments(), unmangleNames);
        if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
            arguments = "*";
        }
        if (node.isDistinct()) {
            arguments = "DISTINCT " + arguments;
        }

        builder.append(formatQualifiedName(node.getName(), escape))
                .append('(').append(arguments).append(')');

        if (node.getWindow().isPresent()) {
            builder.append(" OVER ").append(visitWindow(node.getWindow().get(), unmangleNames));
        }

        return builder.toString();
    }

    private String joinExpressions(List<Expression> expressions, boolean unmangleNames)
    {
        return Joiner.on(", ").join(expressions.stream()
                .map((e) -> process(e, unmangleNames))
                .iterator());
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Boolean unmangleNames)
    {
        return formatSql(node.getBase(), tableNameMapper, null, queryWithTables, escape) + "[" + formatSql(node.getIndex(), tableNameMapper, escape) + "]";
    }

    @Override
    protected String visitSubqueryExpression(SubqueryExpression node, Boolean unmangleNames)
    {
        return "(" + formatSql(node.getQuery(), tableNameMapper, null, queryWithTables, escape) + ")";
    }

    @Override
    protected String visitExists(ExistsPredicate node, Boolean unmangleNames)
    {
        return "EXISTS (" + formatSql(node.getSubquery(), tableNameMapper, escape) + ")";
    }

    @Override
    protected String visitArrayConstructor(ArrayConstructor node, Boolean unmangleNames)
    {
        ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
        for (Expression value : node.getValues()) {
            valueStrings.add(formatSql(value, tableNameMapper, null, queryWithTables, escape));
        }
        return "ARRAY[" + Joiner.on(",").join(valueStrings.build()) + "]";
    }

    @Override
    protected String visitQualifiedNameReference(QualifiedNameReference node, Boolean unmangleNames)
    {
        if (columnNameMapper.isPresent()) {
            return columnNameMapper.get().apply(node.getName());
        }
        return formatQualifiedName(node.getName(), escape);
    }

    private static String formatQualifiedName(QualifiedName name, char escape)
    {
        List<String> parts = new ArrayList<>();
        for (String part : name.getParts()) {
            parts.add(formatIdentifier(part, escape));
        }
        return Joiner.on('.').join(parts);
    }

    @Override
    protected String visitDereferenceExpression(DereferenceExpression node, Boolean unmangleNames)
    {
        String baseString = process(node.getBase(), unmangleNames);
        return baseString + "." + formatIdentifier(node.getFieldName(), escape);
    }

    public static String formatIdentifier(String s, char escape)
    {
        // TODO: handle escaping properly
        return escape + s + escape;
    }

    private static String formatGroupingSet(Set<Expression> groupingSet)
    {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(ex -> ExpressionFormatter.formatExpression(ex, Optional.empty()))
                .iterator()));
    }

    private static String formatGroupingSet(List<QualifiedName> groupingSet)
    {
        return format("(%s)", Joiner.on(", ").join(groupingSet));
    }

    public static String formatGroupBy(List<GroupingElement> groupingElements,
            Function<QualifiedName, String> tableNameMapper,
            Optional<Function<QualifiedName, String>> columnNameMapper, char escape)
    {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                Set<Expression> columns = ImmutableSet.copyOf(((SimpleGroupBy) groupingElement).getColumnExpressions());
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns), tableNameMapper, columnNameMapper, escape);
                }
                else {
                    result = formatGroupingSet(columns);
                }
            }
            else if (groupingElement instanceof GroupingSets) {
                result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                        groupingElement.enumerateGroupingSets().stream()
                                .map(RakamExpressionFormatter::formatGroupingSet)
                                .iterator()));
            }
            else if (groupingElement instanceof Cube) {
                result = format("CUBE %s", formatGroupingSet(((Cube) groupingElement).getColumns()));
            }
            else if (groupingElement instanceof Rollup) {
                result = format("ROLLUP %s", formatGroupingSet(((Rollup) groupingElement).getColumns()));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }
}
