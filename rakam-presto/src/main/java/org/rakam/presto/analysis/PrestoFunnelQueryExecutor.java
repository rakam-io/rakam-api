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
package org.rakam.presto.analysis;

import com.facebook.presto.sql.RakamExpressionFormatter;
import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.CalculatedUserSet;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.AbstractFunnelQueryExecutor;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.PreComputedTableSubQueryVisitor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.util.ValidationUtil.checkCollection;

public class PrestoFunnelQueryExecutor
        extends AbstractFunnelQueryExecutor
{
    private final QueryExecutorService executorService;
    private static final String CONNECTOR_FIELD = "_user";
    private final MaterializedViewService materializedViewService;
    private final ContinuousQueryService continuousQueryService;

    @Inject
    public PrestoFunnelQueryExecutor(QueryExecutorService executorService, QueryExecutor executor, MaterializedViewService materializedViewService, ContinuousQueryService continuousQueryService)
    {
        super(executor);
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
        this.executorService = executorService;
    }

    @Override
    public QueryExecution query(String project, List<FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate, Optional<FunnelWindow> window)
    {
        if (dimension.isPresent() && CONNECTOR_FIELD.equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }

        Set<CalculatedUserSet> calculatedUserSets = new HashSet<>();

        String stepQueries = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(calculatedUserSets, project, CONNECTOR_FIELD, i, window, steps.get(i), dimension, startDate, endDate))
                .collect(Collectors.joining(", "));

        if(calculatedUserSets.size() == steps.size()){
            return super.query(project, steps, dimension, startDate, endDate, window);
        }

        String query;
        if (dimension.isPresent()) {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> format("(SELECT step, (CASE WHEN rank > 15 THEN 'Others' ELSE cast(dimension as varchar) END) as %s," +
                                    " sum(count) FROM (select 'Step %d' as step, dimension, cardinality(merge_sets(%s_set)) count, row_number() OVER(ORDER BY 3 DESC) rank from " +
                                    "step%s %s ORDER BY 4 ASC) GROUP BY 1, 2)",
                            dimension.get(), i + 1, CONNECTOR_FIELD, i, dimension.map(v -> "GROUP BY 2").orElse("")))
                    .collect(Collectors.joining(" UNION ALL ")) + " ORDER BY 1 ASC";
        }
        else {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> format("(SELECT 'Step %d' as step, coalesce(cardinality(merge_sets(%s_set)), 0) count FROM step%d)",
                            i + 1, CONNECTOR_FIELD, i))
                    .collect(Collectors.joining(" UNION ALL ")) + " ORDER BY 1 ASC";
        }

        return new DelegateQueryExecution(executorService.executeQuery(project, "WITH \n" + stepQueries + " " + query),
                result -> {
                    result.setProperty("calculatedUserSets", calculatedUserSets);
                    return result;
                });
    }

    private String convertFunnel(Set<CalculatedUserSet> calculatedUserSets, String project,
            String connectorField,
            int idx,
            Optional<FunnelWindow> window,
            FunnelStep funnelStep,
            Optional<String> dimension,
            LocalDate startDate, LocalDate endDate)
    {
        String timePredicate = format("BETWEEN timestamp '%s' and timestamp '%s' + interval '1' day",
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE));

        Optional<String> joinPreviousStep = idx == 0 ?
                Optional.empty() :
                Optional.of(format("JOIN step%d ON (step%d.date >= step%d.date %s %s)",
                        idx - 1, idx, idx - 1,
                        window.map(v -> String.format("AND step%d.date - interval '%d' %s < step%d.date", idx, v.value, v.type.name().toLowerCase(), idx - 1)).orElse(""),
                        dimension.map(value -> format("AND step%d.dimension = step%d.dimension", idx, idx - 1)).orElse("")));

        Optional<String> preComputedTable = getPreComputedTable(calculatedUserSets, project, funnelStep.getCollection(), connectorField,
                joinPreviousStep, timePredicate, dimension, funnelStep.getExpression(), idx);

        if (preComputedTable.isPresent()) {
            return format("step%s AS (%s)", idx, preComputedTable.get());
        }
        else {
            Optional<String> filterExp = funnelStep.getExpression().map(value -> "AND " + RakamSqlFormatter.formatExpression(value,
                    name -> name.getParts().stream().map(RakamExpressionFormatter::formatIdentifier).collect(Collectors.joining(".")),
                    name -> checkCollection(funnelStep.getCollection()) + "." + name.getParts().stream()
                            .map(RakamExpressionFormatter::formatIdentifier).collect(Collectors.joining("."))));

            String merged = format("step%d.date, %s intersection(merge_sets(step%d.\"%s_set\"), merge_sets(step%d.\"%s_set\")) as %s_set",
                    idx, dimension.map(v -> "step" + idx + ".dimension, ").orElse(""), idx, connectorField, idx - 1, connectorField, connectorField);
            return format("step%d AS (select %s FROM (%s) step%d %s)",
                    idx, idx == 0 ? ("step" + idx + ".*") : merged,
                    format("SELECT cast(_time as date) as date, %s set(%s) as %s_set from %s where _time %s %s group by 1 %s",
                            dimension.map(value -> value + " as dimension,").orElse(""),
                            connectorField, connectorField,
                            checkCollection(funnelStep.getCollection()),
                            timePredicate, filterExp.orElse(""), dimension.map(value -> ", 2").orElse("")),
                    idx,
                    joinPreviousStep.map(v -> v + " GROUP BY 1" + dimension.map(val -> ", 2").orElse("")).orElse(""));
        }
    }

    private Optional<String> getPreComputedTable(
            Set<CalculatedUserSet> calculatedUserSets, String project,
            String collection, String connectorField,
            Optional<String> joinPart,
            String timePredicate, Optional<String> dimension,
            Optional<Expression> filterExpression, int stepIdx)
    {
        String tableNameForCollection = connectorField + "s_daily_" + collection;

        if (filterExpression.isPresent()) {
            try {
                String query = new PreComputedTableSubQueryVisitor(columnName -> {
                    String tableRef = tableNameForCollection + "_by_" + columnName;
                    if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(tableRef))) {
                        return Optional.of("continuous." + checkCollection(tableRef));
                    }
                    else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(tableRef))) {
                        return Optional.of("materialized." + checkCollection(tableRef));
                    }

                    calculatedUserSets.add(new CalculatedUserSet(Optional.of(collection), Optional.of(columnName)));
                    return Optional.empty();
                }).process(filterExpression.get(), false);

                if (dimension.isPresent()) {
                    final boolean[] referenced = {false};

                    new DefaultExpressionTraversalVisitor<Void, Void>()
                    {
                        @Override
                        protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context)
                        {
                            if (node.getName().toString().equals(dimension.get())) {
                                referenced[0] = true;
                            }
                            return null;
                        }
                    }.process(filterExpression.get(), null);

                    if (referenced[0]) {
                        return Optional.of(query);
                    }

                    String tableName = tableNameForCollection + dimension.map(value -> "_by_" + value).orElse("");
                    Optional<String> schema = getSchemaForPreCalculatedTable(project, connectorField, tableName, dimension);

                    if (!schema.isPresent()) {
                        calculatedUserSets.add(new CalculatedUserSet(Optional.of(collection), dimension));
                        return Optional.empty();
                    }

                    return Optional.of("SELECT data.date, data.dimension, data." + CONNECTOR_FIELD + "_set FROM " + schema.get() + "." + tableName + " data " +
                            "JOIN (" + query + ") filter ON (filter.date = data.date)");
                }
                else {
                    return Optional.of(query);
                }
            }
            catch (UnsupportedOperationException e) {
                return Optional.empty();
            }
        }

        String refTable = dimension.map(value -> tableNameForCollection + "_by_" + value).orElse(tableNameForCollection);

        Optional<String> table = getSchemaForPreCalculatedTable(project, connectorField, collection, dimension);
        if (!table.isPresent()) {
            calculatedUserSets.add(new CalculatedUserSet(Optional.of(collection), dimension));
        }
        return table
                .map(value -> generatePreCalculatedTableSql(refTable, value, connectorField,
                        dimension, joinPart, timePredicate, stepIdx));
    }

    private Optional<String> getSchemaForPreCalculatedTable(String project, String connectorField, String collection, Optional<String> dimension)
    {
        String refTable = connectorField + "s_daily_" + collection + dimension.map(value -> "_by_" + value).orElse("");

        if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(refTable))) {
            return Optional.of("continuous");
        }
        else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(refTable))) {
            return Optional.of("materialized");
        }

        return Optional.empty();
    }

    private String generatePreCalculatedTableSql(String table, String schema, String connectorField, Optional<String> dimensionColumn, Optional<String> joinPart, String timePredicate, int stepIdx)
    {
        return format("select step%d.date, %s step%d.%s_set from %s.%s as step%d %s where step%d.date %s",
                stepIdx, dimensionColumn.map(v -> format("step%d.dimension,", stepIdx)).orElse(""),
                stepIdx, connectorField, schema, table, stepIdx,
                joinPart.orElse(""), stepIdx, timePredicate);
    }
}
