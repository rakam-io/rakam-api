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
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.facebook.presto.sql.ExpressionFormatter.formatSortItems;
import static com.facebook.presto.sql.ExpressionFormatter.formatStringLiteral;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;

public final class RakamSqlFormatter
{
    private static final String INDENT = "   ";

    private RakamSqlFormatter() {}

    public static String formatSql(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, 0).process(root, new ArrayList<>());
        return builder.toString();
    }

    public static class Formatter
            extends AstVisitor<Void, List<String>>
    {
        private final StringBuilder builder;
        private final int indent;
        public Formatter(StringBuilder builder, int indent)
        {
            this.builder = builder;
            this.indent = indent;
        }

        @Override
        protected Void visitNode(Node node,List<String> referencedTables)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node,List<String> referencedTables)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node));
            return null;
        }

        @Override
        protected Void visitUnnest(Unnest node,List<String> referencedTables)
        {
            builder.append(node.toString());
            return null;
        }

        @Override
        protected Void visitQuery(Query node,List<String> referencedTables)
        {
            if (node.getWith().isPresent()) {
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
                    appendAliasColumns(builder, query.getColumnNames());
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), referencedTables);
                    referencedTables.add(query.getName());
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            processRelation(node.getQueryBody(), referencedTables);

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + formatSortItems(node.getOrderBy()))
                        .append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            if (node.getApproximate().isPresent()) {
                String confidence = node.getApproximate().get().getConfidence();
                append(indent, "APPROXIMATE AT " + confidence + " CONFIDENCE")
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node,List<String> referencedTables)
        {
            process(node.getSelect(), referencedTables);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), referencedTables);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY " + Joiner.on(", ").join(transform(node.getGroupBy(), ExpressionFormatter::formatExpression)))
                        .append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + formatSortItems(node.getOrderBy()))
                        .append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }
            return null;
        }

        @Override
        protected Void visitSelect(Select node,List<String> referencedTables)
        {
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

                    process(item, referencedTables);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), referencedTables);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node,List<String> referencedTables)
        {
            builder.append(formatExpression(node.getExpression()));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append('"')
                        .append(node.getAlias().get())
                        .append('"'); // TODO: handle quoting properly
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, List<String> referencedTables)
        {
            builder.append(node.toString());

            return null;
        }

        @Override
        protected Void visitTable(Table node,List<String> referencedTables)
        {
            builder.append(node.getName().toString());
            return null;
        }

        @Override
        protected Void visitJoin(Join node,List<String> referencedTables)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), referencedTables);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), referencedTables);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                }
                else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON (")
                            .append(formatExpression(on.getExpression()))
                            .append(")");
                }
                else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node,List<String> referencedTables)
        {
            process(node.getRelation(), referencedTables);

            builder.append(' ')
                    .append(node.getAlias());

            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node,List<String> referencedTables)
        {
            process(node.getRelation(), referencedTables);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            if (node.getColumnsToStratifyOn().isPresent()) {
                builder.append(" STRATIFY ON ")
                        .append(" (")
                        .append(Joiner.on(",").join(node.getColumnsToStratifyOn().get()));
                builder.append(')');
            }

            return null;
        }

        @Override
        protected Void visitValues(Values node,List<String> referencedTables)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row));
                first = false;
            }

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node,List<String> referencedTables)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), referencedTables);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node,List<String> referencedTables)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), referencedTables);

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
        protected Void visitExcept(Except node,List<String> referencedTables)
        {
            processRelation(node.getLeft(), referencedTables);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), referencedTables);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node,List<String> referencedTables)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), referencedTables);

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
        protected Void visitCreateView(CreateView node,List<String> referencedTables)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ")
                    .append(node.getName())
                    .append(" AS\n");

            process(node.getQuery(), referencedTables);

            return null;
        }

        @Override
        protected Void visitDropView(DropView node, List<String> referencedTables)
        {
            builder.append("DROP VIEW ")
                    .append(node.getName());

            return null;
        }

        @Override
        protected Void visitExplain(Explain node,List<String> referencedTables)
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
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), referencedTables);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, List<String> referencedTables)
        {
            builder.append("SHOW CATALOGS");

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, List<String> referencedTables)
        {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, List<String> referencedTables)
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
        protected Void visitShowColumns(ShowColumns node, List<String> referencedTables)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(node.getTable());

            return null;
        }

        @Override
        protected Void visitShowPartitions(ShowPartitions node, List<String> referencedTables)
        {
            builder.append("SHOW PARTITIONS FROM ")
                    .append(node.getTable());

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get()));
            }

            if (!node.getOrderBy().isEmpty()) {
                builder.append(" ORDER BY ")
                        .append(formatSortItems(node.getOrderBy()));
            }

            if (node.getLimit().isPresent()) {
                builder.append(" LIMIT ")
                        .append(node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, List<String> referencedTables)
        {
            builder.append("SHOW FUNCTIONS");

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, List<String> referencedTables)
        {
            builder.append("SHOW SESSION");

            return null;
        }

        @Override
        protected Void visitDropTable(DropTable node, List<String> referencedTables)
        {
            builder.append("DROP TABLE ")
                    .append(node.getTableName());

            return null;
        }

        @Override
        protected Void visitRenameTable(RenameTable node, List<String> referencedTables)
        {
            builder.append("ALTER TABLE ")
                    .append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitInsert(Insert node,List<String> referencedTables)
        {
            builder.append("INSERT INTO ")
                    .append(node.getTarget())
                    .append(" ");

            process(node.getQuery(), referencedTables);

            return null;
        }

        @Override
        public Void visitSetSession(SetSession node, List<String> referencedTables)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void visitResetSession(ResetSession node, List<String> referencedTables)
        {
            builder.append("RESET SESSION ")
                    .append(node.getName());

            return null;
        }

        private void processRelation(Relation relation,List<String> referencedTables)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, referencedTables);
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
}
