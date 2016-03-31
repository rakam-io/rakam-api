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

import com.google.common.primitives.Ints;
import org.rakam.analysis.CalculatedUserSet;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.report.AbstractRetentionQueryExecutor;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.DAY;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.MONTH;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.WEEK;
import static org.rakam.util.ValidationUtil.checkArgument;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PrestoRetentionQueryExecutor extends AbstractRetentionQueryExecutor {

    private final QueryExecutorService executor;

    @Inject
    public PrestoRetentionQueryExecutor(QueryExecutorService executor, Metastore metastore,
                                        MaterializedViewService materializedViewService,
                                        ContinuousQueryService continuousQueryService) {
        super(metastore, materializedViewService, continuousQueryService);
        this.executor = executor;
    }

    public String diffTimestamps(DateUnit dateUnit, String start, String end) {
        return String.format("date_diff('%s', %s, %s)",
                dateUnit.name().toLowerCase(), start, end);
    }

    @Override
    public QueryExecution query(String project, Optional<RetentionAction> firstAction, Optional<RetentionAction> returningAction,
                                DateUnit dateUnit, Optional<String> dimension, int period, LocalDate startDate, LocalDate endDate) {
        checkArgument(period >= 0, "Period must be 0 or a positive value");

        String timeColumn;

        checkTableColumn(CONNECTOR_FIELD, "connector field");

        if (dateUnit == DAY) {
            timeColumn = "cast(%s as date)";
        } else if (dateUnit == WEEK) {
            timeColumn = "cast(date_trunc('week', %s) as date)";
        } else if (dateUnit == MONTH) {
            timeColumn = "cast(date_trunc('month', %s) as date)";
        } else {
            throw new UnsupportedOperationException();
        }

        LocalDate start;
        LocalDate end;
        if (dateUnit == MONTH) {
            start = startDate.withDayOfMonth(1);
            end = endDate.withDayOfMonth(1).plus(1, ChronoUnit.MONTHS);
        } else if (dateUnit == WEEK) {
            TemporalField fieldUS = WeekFields.of(Locale.US).dayOfWeek();
            start = startDate.with(fieldUS, 1);
            end = endDate.with(fieldUS, 1).plus(1, ChronoUnit.MONTHS);
        } else if (dateUnit == DAY) {
            start = startDate;
            end = endDate;
        } else {
            throw new IllegalStateException();
        }
        int range = Math.min(period, Ints.checkedCast(dateUnit.getTemporalUnit().between(start, end)));

        if (range < 0) {
            throw new IllegalArgumentException("startDate must be before endDate.");
        }

        if (range == 0) {
            return QueryExecution.completedQueryExecution(null, QueryResult.empty());
        }

        Set<CalculatedUserSet> missingPreComputedTables = new HashSet<>();

        String firstActionQuery = generateQuery(project, firstAction, CONNECTOR_FIELD, timeColumn, dimension,
                startDate, endDate, missingPreComputedTables);
        String returningActionQuery = generateQuery(project, returningAction, CONNECTOR_FIELD, timeColumn, dimension,
                startDate, endDate, missingPreComputedTables);

        if(firstActionQuery == null || returningActionQuery == null){
            return QueryExecution.completedQueryExecution("", QueryResult.empty());
        }

        String timeSubtraction = diffTimestamps(dateUnit, "data.date", "returning_action.date");

        String dimensionColumn = dimension.isPresent() ? "data.dimension" : "data.date";

        String mergeSetAggregation = dimension.map(v -> "merge_sets").orElse("");
        String query = format("with first_action as (\n" +
                        "  %s\n" +
                        "), \n" +
                        "returning_action as (\n" +
                        "  %s\n" +
                        ") \n" +
                        "select %s, cast(null as bigint) as lead, cardinality(%s(%s_set)) count from first_action data %s union all\n" +
                        "SELECT * FROM (select %s, %s - 1, cardinality_intersection(%s(data.%s_set), %s(returning_action.%s_set)) count \n" +
                        "from first_action data join returning_action on (data.date < returning_action.date AND data.date + interval '%d' day >= returning_action.date) \n" +
                        "%s) ORDER BY 1, 2 NULLS FIRST",
                firstActionQuery, returningActionQuery, dimensionColumn, mergeSetAggregation,
                CONNECTOR_FIELD, dimension.map(v -> "GROUP BY 1").orElse(""), dimensionColumn, timeSubtraction, mergeSetAggregation, CONNECTOR_FIELD,
                mergeSetAggregation, CONNECTOR_FIELD, period,
                dimension.map(v -> "GROUP BY 1, 2").orElse(""));

        return new DelegateQueryExecution(executor.executeQuery(project, query),
                result -> {
                    result.setProperty("calculatedUserSets", missingPreComputedTables);
                    return result;
                });
    }
}

