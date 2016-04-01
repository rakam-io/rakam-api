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
package org.rakam.postgresql.analysis;

import com.google.common.primitives.Ints;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.report.AbstractRetentionQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;

import static java.lang.String.format;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.MONTH;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.WEEK;
import static org.rakam.util.ValidationUtil.checkArgument;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PostgresqlRetentionQueryExecutor extends AbstractRetentionQueryExecutor {

    private final QueryExecutor executor;

    @Inject
    public PostgresqlRetentionQueryExecutor(QueryExecutor executor,
                                            Metastore metastore,
                                            MaterializedViewService materializedViewService,
                                            ContinuousQueryService continuousQueryService) {
        super(metastore, materializedViewService, continuousQueryService);
        this.executor = executor;
    }

    public String diffTimestamps(DateUnit dateUnit, String start, String end) {
        if (dateUnit == WEEK) {
            // Postgresql doesn't support date_part('week', interval).
            return format("cast(date_part('day', age(%s, %s)) as bigint)*7", end, start);
        }
        return format("cast(date_part('%s', age(%s, %s)) as bigint)",
                dateUnit.name().toLowerCase(), end, start);
    }

    @Override
    public QueryExecution query(String project, Optional<RetentionAction> firstAction, Optional<RetentionAction> returningAction, DateUnit dateUnit, Optional<String> dimension, int period, LocalDate startDate, LocalDate endDate) {
        checkArgument(period >= 0, "Period must be 0 or a positive value");
        checkTableColumn(CONNECTOR_FIELD, "connector field");

        String timeColumn = getTimeExpression(dateUnit);

        LocalDate start;
        LocalDate end;
        if (dateUnit == MONTH) {
            start = startDate.withDayOfMonth(1);
            end = endDate.withDayOfMonth(1).plus(1, ChronoUnit.MONTHS);
        } else if (dateUnit == WEEK) {
            TemporalField fieldUS = WeekFields.of(Locale.US).dayOfWeek();
            start = startDate.with(fieldUS, 1);
            end = endDate.with(fieldUS, 1).plus(1, ChronoUnit.MONTHS);
        } else {
            start = startDate;
            end = endDate;
        }
        int range = Math.min(period, Ints.checkedCast(dateUnit.getTemporalUnit().between(start, end)));

        if (range < 0) {
            throw new IllegalArgumentException("startDate must be before endDate.");
        }

        if (range == 0) {
            return QueryExecution.completedQueryExecution(null, QueryResult.empty());
        }

        String firstActionQuery = generateQuery(project, firstAction, CONNECTOR_FIELD, timeColumn, dimension, startDate, endDate, new HashSet<>());
        String returningActionQuery = generateQuery(project, returningAction, CONNECTOR_FIELD, timeColumn, dimension, startDate, endDate, new HashSet<>());

        String timeSubtraction = diffTimestamps(dateUnit, "data.time", "returning_action.time") + "-1";

        String dimensionColumn = dimension.isPresent() ? "data.dimension" : "data.time";

        String query = format("with first_action as (\n" +
                        "  %s\n" +
                        "), \n" +
                        "returning_action as (\n" +
                        "  %s\n" +
                        ") \n" +
                        "select %s, cast(null as bigint) as lead, count(*) count from first_action data group by 1 union all\n" +
                        "select %s, %s, count(*) \n" +
                        "from first_action data join returning_action on (data.time < returning_action.time AND data.%s = returning_action.%s) \n" +
                        "where %s < %d group by 1, 2 ORDER BY 1, 2 NULLS FIRST",
                firstActionQuery, returningActionQuery, dimensionColumn,
                dimensionColumn, timeSubtraction, CONNECTOR_FIELD, CONNECTOR_FIELD,
                timeSubtraction, period);

        return executor.executeRawQuery(query);
    }
}

