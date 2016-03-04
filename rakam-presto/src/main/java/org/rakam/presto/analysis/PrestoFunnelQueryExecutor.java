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

import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.RakamSqlFormatter.ExpressionFormatter;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.RakamSqlFormatter.ExpressionFormatter.formatIdentifier;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public class PrestoFunnelQueryExecutor implements FunnelQueryExecutor {
    private final QueryExecutorService executor;
    private static final String CONNECTOR_FIELD = "_user";
    private final MaterializedViewService materializedViewService;
    private final ContinuousQueryService continuousQueryService;

    @Inject
    public PrestoFunnelQueryExecutor(QueryExecutorService executor, MaterializedViewService materializedViewService, ContinuousQueryService continuousQueryService) {
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
        this.executor = executor;
    }

    public static class CalculatedUserSet {
        public final Optional<String> collection;
        public final Optional<String> dimension;

        public CalculatedUserSet(Optional<String> collection, Optional<String> dimension) {
            this.collection = collection;
            this.dimension = dimension;
        }
    }

    @Override
    public QueryExecution query(String project, List<FunnelQueryExecutor.FunnelStep> steps, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        if (dimension.isPresent() && CONNECTOR_FIELD.equals(dimension.get())) {
            throw new RakamException("Dimension and connector field cannot be equal", HttpResponseStatus.BAD_REQUEST);
        }

        List<CalculatedUserSet> calculatedUserSets = new ArrayList<>();

        String ctes = IntStream.range(0, steps.size())
                .mapToObj(i -> convertFunnel(calculatedUserSets, project, CONNECTOR_FIELD, i, steps.get(i), dimension, startDate, endDate))
                .collect(Collectors.joining(", "));

        String query;
        if (dimension.isPresent()) {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> String.format("(SELECT step, CASE WHEN rank > 15 THEN 'Others' ELSE cast(%s as varchar) END, sum(count) FROM (select 'Step %d' as step, %s, count(*) count, row_number() OVER(ORDER BY 3 DESC) rank from step%s GROUP BY 2 ORDER BY 4 ASC) GROUP BY 1, 2 ORDER BY 3 DESC)",
                            dimension.get(), i + 1, dimension.get(), i))
                    .collect(Collectors.joining(" UNION ALL "));
        } else {
            query = IntStream.range(0, steps.size())
                    .mapToObj(i -> String.format("(SELECT 'Step %d' as step, count(*) count FROM step%s)",
                            i + 1, i))
                    .collect(Collectors.joining(" UNION ALL "));
        }

        return new DelegateQueryExecution(executor.executeQuery(project, "WITH \n" + ctes + " " + query + " ORDER BY 1 ASC"),
                result -> {
                    result.setProperty("calculatedUserSets", calculatedUserSets);
                    return result;
                });
    }

    private String convertFunnel(List<CalculatedUserSet> calculatedUserSets, String project,
                                 String connectorField,
                                 int idx,
                                 FunnelStep funnelStep,
                                 Optional<String> dimension,
                                 LocalDate startDate, LocalDate endDate) {
        String timePredicate = String.format("BETWEEN cast('%s' as date) and cast('%s' as date) + interval '1' day",
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE));

        Optional<String> joinPreviousStep = idx == 0 ?
                Optional.empty() :
                Optional.of(String.format("join step%d on (step%d.%s = step%d.%s)", idx - 1, idx, connectorField, idx - 1, connectorField));

        Optional<String> preComputedTable = getPreComputedTable(project, funnelStep.collection,
                joinPreviousStep, timePredicate, dimension);

        if (preComputedTable.isPresent() && funnelStep.filterExpression == null) {
            return String.format("step%s AS (%s)", idx, preComputedTable.get());
        } else {
            String filterExp = funnelStep.filterExpression != null && !funnelStep.filterExpression.isEmpty() ?
                    "AND " + RakamSqlFormatter.formatExpression(funnelStep.getExpression(),
                            name -> name.getParts().stream().map(ExpressionFormatter::formatIdentifier).collect(Collectors.joining(".")),
                            name -> formatIdentifier(funnelStep.collection) + "." + name.getParts().stream()
                                    .map(ExpressionFormatter::formatIdentifier).collect(Collectors.joining("."))) : "";

            calculatedUserSets.add(new CalculatedUserSet(Optional.of(funnelStep.collection), dimension));

            return String.format("step%d AS (select %s %s from %s as step%d %s where _time %s %s group by 1 %s)",
                    idx,
                    dimension.map(value -> "step" + idx + "." + value + ",").orElse(""),
                    "step" + idx + "." + connectorField,
                    funnelStep.collection,
                    idx,
                    joinPreviousStep.orElse(""),
                    timePredicate,
                    filterExp,
                    dimension.isPresent() ? ", 2" : "");
        }
    }

    private Optional<String> getPreComputedTable(String project, String collection, Optional<String> joinPart, String timePredicate, Optional<String> dimension) {
        String tableName = "_users_daily_" + collection + dimension.map(value -> "_by_" + value).orElse("");

        String dimensionColumn;
        if(dimension.isPresent()) {
            dimensionColumn = dimension.get();
        } else {
            dimensionColumn = "date as time";
        }
        if (continuousQueryService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
            return Optional.of(generatePreCalculatedTableSql(tableName, "continuous", dimensionColumn, joinPart, timePredicate));
        } else if (materializedViewService.list(project).stream().anyMatch(e -> e.tableName.equals(tableName))) {
            return Optional.of(generatePreCalculatedTableSql(tableName, "materialized", dimensionColumn, joinPart, timePredicate));
        }

        return Optional.empty();
    }

    private String generatePreCalculatedTableSql(String table, String schema, String dimensionColumn, Optional<String> joinPart, String timePredicate) {
        return String.format("select %s, _user from %s.%s %s where date %s",
                dimensionColumn, schema, table, joinPart.orElse(""), timePredicate);
    }
}
