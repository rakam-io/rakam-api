package org.rakam.util;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.function.Function;

public class QueryFormatter
        extends RakamSqlFormatter.Formatter
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
    protected Void visitDropTable(DropTable node, Integer indent) {
        builder.append("DROP TABLE ")
                .append(tableNameMapper.apply(node.getTableName()));
        return null;
    }

    @Override
    protected Void visitCreateTable(CreateTable node, Integer indent)
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
