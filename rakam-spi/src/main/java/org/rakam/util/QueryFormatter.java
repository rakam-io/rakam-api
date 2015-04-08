package org.rakam.util;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Table;

import java.util.function.Function;

public class QueryFormatter
        extends SqlFormatter.Formatter
{
    private final StringBuilder builder;
    private final Function<Table, String> tableNameMapper;

    public QueryFormatter(StringBuilder builder, Function<Table, String> tableNameMapper)
    {
        super(builder);
        this.builder = builder;
        this.tableNameMapper = tableNameMapper;
    }

    @Override
    protected Void visitTable(Table node, Integer indent)
    {
        builder.append(tableNameMapper.apply(node));
        return null;
    }

}
