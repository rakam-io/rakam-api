package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.*;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.ExpressionFormatter.formatGroupBy;
import static com.facebook.presto.sql.RakamExpressionFormatter.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class RakamSqlFormatter {
    private static final String INDENT = "   ";
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

    private RakamSqlFormatter() {
    }

    public static String formatSql(Node root, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, tableNameMapper, null, escapeIdentifier).process(root, 0);
        return builder.toString();
    }

    public static String formatSql(Node root, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, char escapeIdentifier) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, tableNameMapper, columnNameMapper, escapeIdentifier).process(root, 0);
        return builder.toString();
    }

    static String formatSql(Node root, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, List<String> ctes, char escapeIdentifier) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, tableNameMapper, columnNameMapper, ctes, escapeIdentifier).process(root, 0);
        return builder.toString();
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier) {
        return new RakamExpressionFormatter(tableNameMapper, Optional.empty(), escapeIdentifier).process(expression, null);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, char escapeIdentifier) {
        return new RakamExpressionFormatter(tableNameMapper, Optional.of(columnNameMapper), escapeIdentifier).process(expression, null);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, char escapeIdentifier) {
        return new RakamExpressionFormatter(tableNameMapper, columnNameMapper, escapeIdentifier).process(expression, null);
    }

    public static String formatExpression(Expression expression, Function<QualifiedName, String> tableNameMapper, Optional<Function<String, String>> columnNameMapper, List<String> queryWithTables, char escapeIdentifier) {
        return new RakamExpressionFormatter(tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier).process(expression, null);
    }

    public static class Formatter
            extends AstVisitor<Void, Integer> {
        private final StringBuilder builder;
        private final Function<QualifiedName, String> tableNameMapper;
        private final Optional<Function<String, String>> columnNameMapper;
        private final char escapeIdentifier;
        private final List<String> queryWithTables;

        public Formatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, List<String> ctes, char escapeIdentifier) {
            this.builder = builder;
            this.tableNameMapper = tableNameMapper;
            this.escapeIdentifier = escapeIdentifier;
            this.queryWithTables = ctes == null ? new ArrayList<>() : ctes;
            this.columnNameMapper = Optional.ofNullable(columnNameMapper);
        }

        public Formatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier) {
            this(builder, tableNameMapper, null, null, escapeIdentifier);
        }

        public Formatter(StringBuilder builder, Function<QualifiedName, String> tableNameMapper, Function<String, String> columnNameMapper, char escapeIdentifier) {
            this(builder, tableNameMapper, columnNameMapper, null, escapeIdentifier);
        }

        public static String formatQuery(Statement query, Function<QualifiedName, String> tableNameMapper, char escapeIdentifier) {
            StringBuilder builder = new StringBuilder();
            new RakamSqlFormatter.Formatter(builder, tableNameMapper, escapeIdentifier).process(query, 1);
            return builder.toString();
        }

        private static String formatName(String name, char escapeIdentifier) {
            if (NAME_PATTERN.matcher(name).matches()) {
                return name;
            }
            return escapeIdentifier + name.replace(new StringBuilder().append(escapeIdentifier), new StringBuilder()
                    .append(escapeIdentifier)
                    .append(escapeIdentifier)) + escapeIdentifier;
        }

        private static String indentString(int indent) {
            return Strings.repeat(INDENT, indent);
        }

        @Override
        protected Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent) {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            return null;
        }

        @Override
        protected Void visitUnnest(Unnest node, Integer indent) {
            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(expression -> formatExpression(expression, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
            return null;
        }

        @Override
        protected Void visitLateral(Lateral node, Integer indent) {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
            return null;
        }

        @Override
        protected Void visitPrepare(Prepare node, Integer indent) {
            append(indent, "PREPARE ");
            builder.append(node.getName());
            builder.append(" FROM");
            builder.append("\n");
            process(node.getStatement(), indent + 1);
            return null;
        }

        @Override
        protected Void visitDeallocate(Deallocate node, Integer indent) {
            append(indent, "DEALLOCATE PREPARE ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitExecute(Execute node, Integer indent) {
            append(indent, "EXECUTE ");
            builder.append(node.getName());
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append(" USING ");
                Joiner.on(", ").appendTo(builder, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier);
            }
            return null;
        }

        @Override
        protected Void visitDescribeOutput(DescribeOutput node, Integer indent) {
            append(indent, "DESCRIBE OUTPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitDescribeInput(DescribeInput node, Integer indent) {
            append(indent, "DESCRIBE INPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent) {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                queryWithTables.addAll(with.getQueries().stream()
                        .map(WithQuery::getName)
                        .map(Identifier::getValue)
                        .collect(Collectors.toList()));

                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, formatExpression(query.getName(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
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
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
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
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + formatGroupBy(node.getGroupBy().get().getGroupingElements())).append('\n');
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
        protected Void visitOrderBy(OrderBy node, Integer indent) {
            append(indent, formatOrderBy(node, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent) {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            } else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent) {
            builder.append(formatExpression(node.getExpression(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append(formatExpression(node.getAlias().get(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context) {
            builder.append(node.toString());

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent) {

            if (!node.getName().getPrefix().isPresent() && queryWithTables.contains(node.getName().getSuffix())) {
                builder.append(formatName(node.getName().toString(), escapeIdentifier));
                return null;
            }

            builder.append(tableNameMapper.apply(node.getName()));

            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent) {
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
            } else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                } else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON ")
                            .append(formatExpression(on.getExpression(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
                } else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
            process(node.getRelation(), indent);

            builder.append(' ')
                    .append(formatExpression(node.getAlias(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent) {
            process(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            return null;
        }

        @Override
        protected Void visitValues(Values node, Integer indent) {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent) {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent) {
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
        protected Void visitExcept(Except node, Integer indent) {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent) {
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
        protected Void visitCreateView(CreateView node, Integer indent) {
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

        @Override
        protected Void visitDropView(DropView node, Integer context) {
            builder.append("DROP VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName());

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent) {
            builder.append("EXPLAIN ");
            if (node.isAnalyze()) {
                builder.append("ANALYZE ");
            }

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                } else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                } else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer context) {
            builder.append("SHOW CATALOGS");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer context) {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer context) {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent(value ->
                    builder.append(" FROM ")
                            .append(formatName(value)));

            node.getLikePattern().ifPresent(value ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowCreate(ShowCreate node, Integer context) {
            if (node.getType() == ShowCreate.Type.TABLE) {
                builder.append("SHOW CREATE TABLE ")
                        .append(formatName(node.getName()));
            } else if (node.getType() == ShowCreate.Type.VIEW) {
                builder.append("SHOW CREATE VIEW ")
                        .append(formatName(node.getName()));
            }

            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer context) {
            builder.append("SHOW COLUMNS FROM ")
                    .append(formatName(node.getTable()));

            return null;
        }

        @Override
        protected Void visitShowStats(ShowStats node, Integer context) {
            builder.append("SHOW STATS FOR ");
            process(node.getRelation(), 0);
            builder.append("");
            return null;
        }

        @Override
        protected Void visitShowPartitions(ShowPartitions node, Integer context) {
            builder.append("SHOW PARTITIONS FROM ")
                    .append(formatName(node.getTable()));

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            }

            if (!node.getOrderBy().isEmpty()) {
                builder.append(" ORDER BY ")
                        .append(formatSortItems(node.getOrderBy(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            }

            if (node.getLimit().isPresent()) {
                builder.append(" LIMIT ")
                        .append(node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer context) {
            builder.append("SHOW FUNCTIONS");

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, Integer context) {
            builder.append("SHOW SESSION");

            return null;
        }

        @Override
        protected Void visitDelete(Delete node, Integer context) {
            builder.append("DELETE FROM ")
                    .append(formatName(node.getTable().getName()));

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));
            }

            return null;
        }

        @Override
        protected Void visitCreateSchema(CreateSchema node, Integer context) {
            builder.append("CREATE SCHEMA ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()));
            builder.append(formatProperties(node.getProperties()));

            return null;
        }

        @Override
        protected Void visitDropSchema(DropSchema node, Integer context) {
            builder.append("DROP SCHEMA ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");

            return null;
        }

        private void appendAliasColumns(StringBuilder builder, List<Identifier> columns) {
            if ((columns != null) && (!columns.isEmpty())) {
                String formattedColumns = columns.stream()
                        .map(name -> formatExpression(name, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                        .collect(Collectors.joining(", "));

                builder.append(" (")
                        .append(formattedColumns)
                        .append(')');
            }
        }

        @Override
        protected Void visitRenameSchema(RenameSchema node, Integer context) {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatExpression(node.getTarget(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));

            return null;
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent) {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName()));

            if (node.getColumnAliases().isPresent()) {
                String columnList = node.getColumnAliases().get().stream().map(element -> formatExpression(element, tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier)).collect(joining(", "));
                builder.append(format("( %s )", columnList));
            }

            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT " + formatStringLiteral(node.getComment().get()));
            }

            builder.append(formatProperties(node.getProperties()));

            builder.append(" AS ");
            process(node.getQuery(), indent);

            if (!node.isWithData()) {
                builder.append(" WITH NO DATA");
            }

            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Integer indent) {
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            String tableName = formatName(node.getName());
            builder.append(tableName).append(" (\n");

            String elementIndent = indentString(indent + 1);
            String columnList = node.getElements().stream()
                    .map(element -> {
                        if (element instanceof ColumnDefinition) {
                            ColumnDefinition column = (ColumnDefinition) element;
                            return elementIndent + formatExpression(column.getName(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier) + " " + column.getType() +
                                    column.getComment()
                                            .map(comment -> " COMMENT " + formatStringLiteral(comment))
                                            .orElse("");
                        }
                        if (element instanceof LikeClause) {
                            LikeClause likeClause = (LikeClause) element;
                            StringBuilder builder = new StringBuilder(elementIndent);
                            builder.append("LIKE ")
                                    .append(formatName(likeClause.getTableName()));
                            if (likeClause.getPropertiesOption().isPresent()) {
                                builder.append(" ")
                                        .append(likeClause.getPropertiesOption().get().name())
                                        .append(" PROPERTIES");
                            }
                            return builder.toString();
                        }
                        throw new UnsupportedOperationException("unknown table element: " + element);
                    })
                    .collect(joining(",\n"));
            builder.append(columnList);
            builder.append("\n").append(")");

            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT " + formatStringLiteral(node.getComment().get()));
            }

            builder.append(formatProperties(node.getProperties()));

            return null;
        }

        private String formatProperties(List<Property> properties) {
            if (properties.isEmpty()) {
                return "";
            }
            String propertyList = properties.stream()
                    .map(element -> INDENT +
                            formatExpression(element.getName(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier) + " = " +
                            formatExpression(element.getValue(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier))
                    .collect(joining(",\n"));

            return "\nWITH (\n" + propertyList + "\n)";
        }

        private String formatName(QualifiedName name) {
            return name.getOriginalParts().stream()
                    .map(e -> formatName(e, escapeIdentifier))
                    .collect(joining("."));
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer context) {
            builder.append("DROP TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTableName());

            return null;
        }

        @Override
        protected Void visitRenameTable(RenameTable node, Integer context) {
            builder.append("ALTER TABLE ")
                    .append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitRenameColumn(RenameColumn node, Integer context) {
            builder.append("ALTER TABLE ")
                    .append(node.getTable())
                    .append(" RENAME COLUMN ")
                    .append(node.getSource())
                    .append(" TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitDropColumn(DropColumn node, Integer context) {
            builder.append("ALTER TABLE ")
                    .append(formatName(node.getTable()))
                    .append(" DROP COLUMN ")
                    .append(formatExpression(node.getColumn(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));

            return null;
        }

        @Override
        protected Void visitAddColumn(AddColumn node, Integer indent) {
            builder.append("ALTER TABLE ")
                    .append(node.getName())
                    .append(" ADD COLUMN ")
                    .append(node.getColumn().getName())
                    .append(" ")
                    .append(node.getColumn().getType());

            return null;
        }

        @Override
        protected Void visitInsert(Insert node, Integer indent) {
            builder.append("INSERT INTO ")
                    .append(node.getTarget())
                    .append(" ");

            if (node.getColumns().isPresent()) {
                builder.append("(")
                        .append(Joiner.on(", ").join(node.getColumns().get()))
                        .append(") ");
            }

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        public Void visitSetSession(SetSession node, Integer context) {
            builder.append("SET SESSION ")
                    .append(node.getName())
                    .append(" = ")
                    .append(formatExpression(node.getValue(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));

            return null;
        }

        @Override
        public Void visitResetSession(ResetSession node, Integer context) {
            builder.append("RESET SESSION ")
                    .append(node.getName());

            return null;
        }

        @Override
        protected Void visitCallArgument(CallArgument node, Integer indent) {
            if (node.getName().isPresent()) {
                builder.append(node.getName().get())
                        .append(" => ");
            }
            builder.append(formatExpression(node.getValue(), tableNameMapper, columnNameMapper, queryWithTables, escapeIdentifier));

            return null;
        }

        @Override
        protected Void visitCall(Call node, Integer indent) {
            builder.append("CALL ")
                    .append(node.getName())
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
        protected Void visitRow(Row node, Integer indent) {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Expression item : node.getItems()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(item, indent);
                firstItem = false;
            }
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitStartTransaction(StartTransaction node, Integer indent) {
            builder.append("START TRANSACTION");

            Iterator<TransactionMode> iterator = node.getTransactionModes().iterator();
            while (iterator.hasNext()) {
                builder.append(" ");
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
            return null;
        }

        @Override
        protected Void visitIsolationLevel(Isolation node, Integer indent) {
            builder.append("ISOLATION LEVEL ").append(node.getLevel().getText());
            return null;
        }

        @Override
        protected Void visitTransactionAccessMode(TransactionAccessMode node, Integer context) {
            builder.append(node.isReadOnly() ? "READ ONLY" : "READ WRITE");
            return null;
        }

        @Override
        protected Void visitCommit(Commit node, Integer context) {
            builder.append("COMMIT");
            return null;
        }

        @Override
        protected Void visitRollback(Rollback node, Integer context) {
            builder.append("ROLLBACK");
            return null;
        }

        @Override
        public Void visitGrant(Grant node, Integer indent) {
            builder.append("GRANT ");

            if (node.getPrivileges().isPresent()) {
                builder.append(node.getPrivileges().get().stream()
                        .collect(joining(", ")));
            } else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ");
            if (node.isTable()) {
                builder.append("TABLE ");
            }
            builder.append(node.getTableName())
                    .append(" TO ")
                    .append(node.getGrantee());
            if (node.isWithGrantOption()) {
                builder.append(" WITH GRANT OPTION");
            }

            return null;
        }

        @Override
        public Void visitRevoke(Revoke node, Integer indent) {
            builder.append("REVOKE ");

            if (node.isGrantOptionFor()) {
                builder.append("GRANT OPTION FOR ");
            }

            if (node.getPrivileges().isPresent()) {
                builder.append(node.getPrivileges().get().stream()
                        .collect(joining(", ")));
            } else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ");
            if (node.isTable()) {
                builder.append("TABLE ");
            }
            builder.append(node.getTableName())
                    .append(" FROM ")
                    .append(node.getGrantee());

            return null;
        }

        @Override
        public Void visitShowGrants(ShowGrants node, Integer indent) {
            builder.append("SHOW GRANTS ");

            if (node.getTableName().isPresent()) {
                builder.append("ON ");

                if (node.getTable()) {
                    builder.append("TABLE ");
                }
                builder.append(node.getTableName().get());
            }

            return null;
        }

        private void processRelation(Relation relation, Integer indent) {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(tableNameMapper.apply(((Table) relation).getName()))
                        .append('\n');
            } else {
                process(relation, indent);
            }
        }

        private StringBuilder append(int indent, String value) {
            return builder.append(indentString(indent))
                    .append(value);
        }
    }
}
