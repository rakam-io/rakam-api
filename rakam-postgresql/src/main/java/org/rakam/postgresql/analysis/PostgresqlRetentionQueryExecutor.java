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
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.report.AbstractRetentionQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.MONTH;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.WEEK;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.ValidationUtil.checkArgument;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PostgresqlRetentionQueryExecutor extends AbstractRetentionQueryExecutor {

    private final QueryExecutorService executor;
    private final Metastore metastore;

    @Inject
    public PostgresqlRetentionQueryExecutor(QueryExecutorService executor,
                                            Metastore metastore) {
        this.executor = executor;
        this.metastore = metastore;
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
    public QueryExecution query(String project, Optional<RetentionAction> firstAction,
                                Optional<RetentionAction> returningAction, DateUnit dateUnit,
                                Optional<String> dimension, Optional<Integer> period,
                                LocalDate startDate, LocalDate endDate) {
        period.ifPresent(e -> checkArgument(e >= 0, "Period must be 0 or a positive value"));
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

        Optional<Integer> range = period.map(v -> Math.min(v, Ints.checkedCast(dateUnit.getTemporalUnit().between(start, end))));

        if (range.isPresent() && range.get() < 0) {
            throw new IllegalArgumentException("startDate must be before endDate.");
        }

        if (range.isPresent() && range.get() < 0) {
            return QueryExecution.completedQueryExecution(null, QueryResult.empty());
        }

        String firstActionQuery = generateQuery(project, firstAction, CONNECTOR_FIELD, timeColumn, dimension, startDate, endDate);
        String returningActionQuery = generateQuery(project, returningAction, CONNECTOR_FIELD, timeColumn, dimension, startDate, endDate);

        String timeSubtraction = diffTimestamps(dateUnit, "data.date", "returning_action.date") + "-1";
        String dimensionColumn = dimension.isPresent() ? "data.dimension" : "data.date";

        String query = format("with first_action as (\n" +
                        "  %s\n" +
                        "), \n" +
                        "returning_action as (\n" +
                        "  %s\n" +
                        ") \n" +
                        "select %s, cast(null as bigint) as lead, count(distinct data.%s) count from first_action data group by 1 union all\n" +
                        "select %s, %s, count(distinct returning_action.%s) \n" +
                        "from first_action data join returning_action on (data.date < returning_action.date AND data.%s = returning_action.%s) \n" +
                        "%s group by 1, 2 ORDER BY 1, 2 NULLS FIRST",
                firstActionQuery, returningActionQuery, dimensionColumn, CONNECTOR_FIELD,
                dimensionColumn, timeSubtraction, CONNECTOR_FIELD, CONNECTOR_FIELD, CONNECTOR_FIELD,
                range.map(v -> String.format("where %s < %d", timeSubtraction, v)).orElse(""));

        return executor.executeQuery(project, query);
    }

    private String generateQuery(String project,
                                 Optional<RetentionAction> retentionAction,
                                 String connectorField,
                                 String timeColumn,
                                 Optional<String> dimension,
                                 LocalDate startDate,
                                 LocalDate endDate) {

        String timePredicate = String.format("between date '%s' and date '%s' + interval '1' day",
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE));

        if (!retentionAction.isPresent()) {
            Map<String, List<SchemaField>> collections = metastore.getCollections(project);
            if (!collections.entrySet().stream().anyMatch(e -> e.getValue().stream().anyMatch(s -> s.getName().equals("_user")))) {
                return format("select cast(null as date) as date, %s cast(null as text) as %s",
                        dimension.isPresent() ? checkTableColumn(dimension.get(), "dimension") + " as dimension, " : "",
                        connectorField);
            }

            boolean isText = collections.entrySet().stream()
                    .anyMatch(e -> e.getValue().stream().anyMatch(z -> z.getType().equals(STRING)));

            return collections.entrySet().stream()
                    .filter(entry -> entry.getValue().stream().anyMatch(e -> e.getName().equals("_user")))
                    .map(collection -> getTableSubQuery(collection.getKey(), connectorField, Optional.of(isText), timeColumn,
                            dimension, timePredicate, Optional.empty()))
                    .collect(Collectors.joining(" union all "));
        } else {
            String collection = retentionAction.get().collection();

            return getTableSubQuery(collection, connectorField, Optional.empty(),
                    timeColumn, dimension, timePredicate, retentionAction.get().filter());
        }
    }
}