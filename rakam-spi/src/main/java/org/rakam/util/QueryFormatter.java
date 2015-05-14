package org.rakam.util;

import com.facebook.presto.sql.SQLFormatter;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Table;

import java.util.function.Function;

public class QueryFormatter
        extends SQLFormatter.Formatter
{
    private final StringBuilder builder;
    private final Function<QualifiedName, String> tableNameMapper;

    public QueryFormatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper)
    {
        super(builder);
        this.builder = builder;
        this.tableNameMapper = tableNameMapper;
    }

    @Override
    protected Void visitTable(Table node, Integer indent)
    {
        builder.append(tableNameMapper.apply(node.getName()));
        return null;
    }

    @Override
    protected Void visitCreateTable(CreateTable node, Integer indent)
    {
        builder.append("CREATE TABLE ")
                .append(tableNameMapper.apply(node.getName()))
                .append(" AS ");

        process(node.getQuery(), indent);

        return null;
    }
}
