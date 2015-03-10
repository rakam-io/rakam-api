package org.rakam.analysis.query;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Strings;

import java.util.function.Function;

public class QueryFormatter
        extends SqlFormatter.Formatter
{
    private final StringBuilder builder;
    private static final String INDENT = "   ";
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


//    @Override
//    protected Void visitSingleColumn(SingleColumn node, Integer indent)
//    {
//        builder.append(formatExpression(node.getExpression()));
//        builder.append(' ')
//                .append('"')
//                .append(node.getAlias().isPresent() ? node.getAlias().get() : "col1")
//                .append('"');
//        return null;
//    }

    private StringBuilder append(int indent, String value)
    {
        return builder.append(indentString(indent))
                .append(value);
    }

    private static String indentString(int indent)
    {
        return Strings.repeat(INDENT, indent);
    }
}
