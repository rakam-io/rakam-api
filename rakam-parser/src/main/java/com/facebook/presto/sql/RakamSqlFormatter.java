/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.ExpressionFormatter.formatStringLiteral;
import static com.facebook.presto.sql.RakamExpressionFormatter.formatGroupBy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.joining;

public final class RakamSqlFormatter
{
    private static final String INDENT = "   ";
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

    private RakamSqlFormatter()
    {
    }

    public static String formatSql(Node root, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, tableNameMapper, null, escapeIdentifier).process(root, 0);
        return builder.toString();
    }

    public static String formatSql(Node root, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, char escapeIdentifier)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, tableNameMapper, columnNameMapper, escapeIdentifier).process(root, 0);
        return builder.toString();
    }

    static String formatSql(Node root, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, List<String> ctes, char escapeIdentifier)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, tableNameMapper, columnNameMapper, ctes, escapeIdentifier).process(root, 0);
        return builder.toString();
    }

    public static class Formatter
            extends AstVisitor<Void, Integer>
    {
        private final StringBuilder builder;
        private final Function<QualifiedName, String> tableNameMapper;
        private final Optional<Function<String, String>> columnNameMapper;
        private final char escapeIdentifier;
        private final List<String> queryWithTables;

        private Formatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, List<String> ctes, char escapeIdentifier)
        {
            this.builder = builder;
            this.tableNameMapper = tableNameMapper;
            this.escapeIdentifier = escapeIdentifier;
            this.queryWithTables = ctes == null ? new ArrayList<>() : ctes;
            this.columnNameMapper = Optional.ofNullable(columnNameMapper);
        }

        public Formatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier)
        {
            this(builder, tableNameMapper, null, null, escapeIdentifier);
        }

        public Formatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, char escapeIdentifier)
        {
            this(builder, tableNameMapper, columnNameMapper, null, escapeIdentifier);
        }

        public static String format(Statement query, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier)
        {
            StringBuilder builder = new StringBuilder();
            new RakamSqlFormatter.Formatter(builder, tableNameMapper, escapeIdentifier).process(query, 1);
            return builder.toString();
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            return null;
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent)
        {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(node.getName());

            appendTableProperties(builder, node.getProperties());

            builder.append(" AS ");
            process(node.getQuery(), indent);

            if (!node.isWithData()) {
                builder.append(" WITH NO DATA");
            }

            return null;
        }

        private void appendTableProperties(StringBuilder builder, Map<String, Expression> properties)
        {
            if (!properties.isEmpty()) {
                builder.append("\nWITH (\n");
                // Always output the table properties in sorted order
                String propertyList = ImmutableSortedMap.copyOf(properties).entrySet().stream()
                        .map(entry -> INDENT + formatName(entry.getKey()) + " = " + formatExpression(entry.getValue(),
                                tableNameMapper, columnNameMapper, queryWithTables,
                                escapeIdentifier))
                        .collect(joining(",\n"));
                builder.append(propertyList);
                builder.append("\n").append(")");
            }
        }

        private String formatName(String name)
        {
            if (NAME_PATTERN.matcher(name).matches()) {
                return name;
            }
            return escapeIdentifier + name + escapeIdentifier;
        }

        @Override
        protected Void visitUnnest(Unnest node, Integer indent)
        {
            builder.append("UNNEST(");

            builder.append(node.getExpressions().stream().map(e ->
                    formatExpression(e, tableNameMapper, columnNameMapper, queryWithTables,
                            escapeIdentifier)).collect(Collectors.joining(", ")));
            builder.append(")");

            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
            return null;
        }

        @Override
        protected Void visitLateral(Lateral node, Integer indent)
        {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (node.getWith().isPresent()) {
                queryWithTables.addAll(node.getWith().get().getQueries().stream()
                        .map(WithQuery::getName).collect(Collectors.toList()));
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, query.getName());
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            processRelation(node.getQueryBody(), indent);

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                        .append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + formatGroupBy(node.getGroupBy().get().getGroupingElements(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier)).append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                        .append('\n');
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Integer indent)
        {
            append(indent, RakamExpressionFormatter.formatOrderBy(node, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                    .append('\n');
            return null;
        }

        String formatSortItems(List<SortItem> sortItems, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, char escapeIdentifier)
        {
            return Joiner.on(", ").join(sortItems.stream()
                    .map(sortItemFormatterFunction(tableNameMapper, columnNameMapper, escapeIdentifier))
                    .iterator());
        }

        private Function<SortItem, String> sortItemFormatterFunction(Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, char escapeIdentifier)
        {
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
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append('\n')
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            builder.append(formatExpression(node.getExpression(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append(escapeIdentifier)
                        .append(node.getAlias().get())
                        .append(escapeIdentifier); // TODO: handle quoting properly
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context)
        {
            builder.append(node.toString());

            return null;
        }

        @Override
        protected Void visitCallArgument(CallArgument node, Integer indent)
        {
            if (node.getName().isPresent()) {
                builder.append(node.getName().get())
                        .append(" => ");
            }
            builder.append(formatExpression(node.getValue(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));

            return null;
        }

        @Override
        protected Void visitCall(Call node, Integer indent)
        {
            builder.append("CALL ")
                    .append(tableNameMapper.apply(node.getName()))
                    .append("(");

            Iterator<CallArgument> arguments = node.getArguments().iterator();
            while (arguments.hasNext()) {
                process(arguments.next(), indent);
                if (arguments.hasNext()) {
                    builder.append(", ");
                }
            }

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            if (!node.getName().getPrefix().isPresent() && queryWithTables.contains(node.getName().getSuffix())) {
                builder.append(node.getName().toString());
                return null;
            }
            builder.append(tableNameMapper.apply(node.getName()));
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(')');
                }
                else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON (")
                            .append(formatExpression(on.getExpression(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                            .append(')');
                }
                else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(')');
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(' ')
                    .append(node.getAlias());

            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            return null;
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append("(" + formatExpression(row, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier) + ")");
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitCreateView(CreateView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ")
                    .append(formatName(node.getName()))
                    .append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        private String formatName(QualifiedName name)
        {
            return name.getOriginalParts().stream()
                    .map(this::formatName)
                    .collect(joining("."));
        }

        @Override
        protected Void visitDropView(DropView node, Integer context)
        {
            builder.append("DROP VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName());

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                }
                else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                }
                else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append('(');
                Joiner.on(", ").appendTo(builder, options);
                builder.append(')');
            }

            builder.append('\n');

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer context)
        {
            builder.append("SHOW CATALOGS");

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer context)
        {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer context)
        {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent((value) ->
                    builder.append(" FROM ")
                            .append(value));

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer context)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(node.getTable());

            return null;
        }

        @Override
        protected Void visitShowPartitions(ShowPartitions node, Integer context)
        {
            builder.append("SHOW PARTITIONS FROM ")
                    .append(node.getTable());

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            }

            if (!node.getOrderBy().isEmpty()) {
                builder.append(" ORDER BY ")
                        .append(formatSortItems(node.getOrderBy(), tableNameMapper, columnNameMapper, escapeIdentifier));
            }

            if (node.getLimit().isPresent()) {
                builder.append(" LIMIT ")
                        .append(node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer context)
        {
            builder.append("SHOW FUNCTIONS");

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, Integer context)
        {
            builder.append("SHOW SESSION");

            return null;
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
        }

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

    private static void appendAliasColumns(StringBuilder builder, List<String> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, columns);
            builder.append(')');
        }
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier)
    {
        return new RakamExpressionFormatter(tableNameMapper, Optional.empty(), escapeIdentifier).process(expression, null);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, char escapeIdentifier)
    {
        return new RakamExpressionFormatter(tableNameMapper, Optional.of(columnNameMapper), escapeIdentifier).process(expression, null);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, char escapeIdentifier)
    {
        return new RakamExpressionFormatter(tableNameMapper, columnNameMapper, escapeIdentifier).process(expression, null);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, List<String> queryWithTables, char escapeIdentifier)
    {
        return new RakamExpressionFormatter(tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier).process(expression, null);
    }
}
