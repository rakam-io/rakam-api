package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class RakamExpressionFormatter extends com.facebook.presto.sql.ExpressionFormatter.Formatter {

    private final Function<QualifiedName, String> tableNameMapper;
    private final Optional<Function<QualifiedName, String>> columnNameMapper;

    public RakamExpressionFormatter(Function<QualifiedName, String> tableNameMapper,
                                    Optional<Function<QualifiedName, String>> columnNameMapper) {
        this.tableNameMapper = tableNameMapper;
        this.columnNameMapper = columnNameMapper;
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Boolean unmangleNames)
    {
        return RakamSqlFormatter.formatSql(node.getBase(), tableNameMapper) + "[" + RakamSqlFormatter.formatSql(node.getIndex(), tableNameMapper) + "]";
    }

    @Override
    protected String visitSubqueryExpression(SubqueryExpression node, Boolean unmangleNames)
    {
        return "(" + RakamSqlFormatter.formatSql(node.getQuery(), tableNameMapper) + ")";
    }

    @Override
    protected String visitExists(ExistsPredicate node, Boolean unmangleNames)
    {
        return "EXISTS (" + RakamSqlFormatter.formatSql(node.getSubquery(), tableNameMapper) + ")";
    }

    @Override
    protected String visitArrayConstructor(ArrayConstructor node, Boolean unmangleNames)
    {
        ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
        for (Expression value : node.getValues()) {
            valueStrings.add(RakamSqlFormatter.formatSql(value, tableNameMapper));
        }
        return "ARRAY[" + Joiner.on(",").join(valueStrings.build()) + "]";
    }

    @Override
    protected String visitQualifiedNameReference(QualifiedNameReference node, Boolean unmangleNames)
    {
        if(columnNameMapper.isPresent()) {
            return columnNameMapper.get().apply(node.getName());
        }
        return formatQualifiedName(node.getName());
    }

    private static String formatQualifiedName(QualifiedName name)
    {
        List<String> parts = new ArrayList<>();
        for (String part : name.getParts()) {
            parts.add(formatIdentifier(part));
        }
        return Joiner.on('.').join(parts);
    }

    public static String formatIdentifier(String s)
    {
        // TODO: handle escaping properly
        return '"' + s + '"';
    }
}
