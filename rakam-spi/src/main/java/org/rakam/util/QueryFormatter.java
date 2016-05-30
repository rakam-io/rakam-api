package org.rakam.util;

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryFormatter
        extends RakamSqlFormatter.Formatter
{
    private final StringBuilder builder;
    private final BiFunction<QualifiedName, StringBuilder, String> tableNameMapper;
    private final List<String> queryWithTables;

    public QueryFormatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper)
    {
        super(builder, tableNameMapper, null);
        this.builder = builder;
        this.queryWithTables = new ArrayList<>();
        this.tableNameMapper = (key, ctx) -> tableNameMapper.apply(key);
    }

    public static String format(Statement query, Function<QualifiedName, String> tableNameMapper) {
        StringBuilder builder = new StringBuilder();
        new QueryFormatter(builder, tableNameMapper).process(query, 1);
        return builder.toString();
    }

    @Override
    protected Void visitQuery(Query node, Integer indent) {
        if (node.getWith().isPresent()) {
            queryWithTables.addAll(node.getWith().get().getQueries().stream()
                    .map(WithQuery::getName).collect(Collectors.toList()));
        }
        return super.visitQuery(node, indent);
    }

    @Override
    protected Void visitTable(Table node, Integer indent)
    {
        if(!node.getName().getPrefix().isPresent() && queryWithTables.contains(node.getName().getSuffix())) {
            builder.append(node.getName().toString());
            return null;
        }
        builder.append(tableNameMapper.apply(node.getName(), builder));
        return null;
    }

    @Override
    protected Void visitDropTable(DropTable node, Integer indent) {
        builder.append("DROP TABLE ")
                .append(tableNameMapper.apply(node.getTableName(), builder));
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
