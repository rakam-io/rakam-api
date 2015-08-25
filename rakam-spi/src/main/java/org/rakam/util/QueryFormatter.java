package org.rakam.util;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.function.Function;

public class QueryFormatter
        extends RakamSqlFormatter.Formatter
{
    private final StringBuilder builder;
    private final Function<QualifiedName, String> tableNameMapper;

    public QueryFormatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper)
    {
        super(builder, 0);
        this.builder = builder;
        this.tableNameMapper = tableNameMapper;
    }

    @Override
    protected Void visitTable(Table node, List<String> referencedTables)
    {
        if(!referencedTables.contains(node.getName().toString())) {
            builder.append(tableNameMapper.apply(node.getName()));
        }else {
            builder.append(node.getName());
        }
        return null;
    }


    @Override
    protected Void visitCreateTable(CreateTable node, List<String> referencedTables)
    {
        this.builder.append("CREATE TABLE ");
        if(node.isNotExists()) {
            this.builder.append("IF NOT EXISTS ");
        }

        this.builder.append(node.getName()).append(" (");
        Joiner.on(", ").appendTo(this.builder, Iterables.transform(node.getElements(),
                (element) -> element.getName() + " " + element.getType()));
        this.builder.append(")");
        return null;
    }
}
