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

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.AbstractRetentionQueryExecutor;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static com.google.common.primitives.Ints.checkedCast;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.MONTH;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.WEEK;
import static org.rakam.collection.FieldType.INTEGER;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.*;

public class PostgresqlRetentionQueryExecutor
        extends AbstractRetentionQueryExecutor {
    private static String PL_PGGSQL_RETENTION_AGGREGATE_FUNCTION = "create or replace function analyze_retention_intermediate(arr integer[], ff boolean[]) returns integer[] volatile language plpgsql as $$\n" +
            "DECLARE \n" +
            " i int;\n" +
            "begin\n" +
            " if ff is null then\n" +
            "    return arr;\n" +
            " end if;\n" +
            " \n" +
            "FOR i IN 1 .. array_upper(ff, 1)\n" +
            " LOOP\n" +
            "   if ff[i] = true then \n" +
            "     if arr[i] is null then \n" +
            "       arr[i] := 1;\n" +
            "     ELSE\n" +
            "       arr[i] := (arr[i]+1);\n" +
            "     end if;\n" +
            "   end if;\n" +
            "END LOOP;\n" +
            "return arr;\n" +
            "END\n" +
            "$$;";
    private static String PL_PGGSQL_RETENTION_TIMELINE_FUNCTION =
            "create or replace function generate_timeline(start date, arr date[], durationmillis bigint, max_step integer) returns boolean[] volatile language plpgsql as $$\n" +
                    "DECLARE \n" +
                    " steps boolean[];\n" +
                    " value int;\n" +
                    " gap int;\n" +
                    " item date;\n" +
                    "BEGIN\n" +
                    " if arr is null then\n" +
                    "  return null;\n" +
                    " end if;\n" +
                    "\n" +
                    "-- substracting dates returns an integer that represents date diff\n" +
                    "durationmillis := durationmillis / 86400000; \n" +
                    "FOREACH item IN ARRAY arr\n" +
                    "LOOP\n" +
                    "   value := (item - start);\n" +
                    "\n" +
                    "   if value < 0 then \n" +
                    "      continue; \n" +
                    "   end if;\n" +
                    "   \n" +
                    "   gap := value / durationmillis;\n" +
                    "   if gap > max_step then \n" +
                    "      EXIT; \n" +
                    "   end if;\n" +
                    "\n" +
                    "   if steps is null then\n" +
                    "       steps = cast(ARRAY[] as boolean[]);\n" +
                    "       --steps = new Array(Math.min(max_step, arr.length))\n" +
                    "   end if;\n" +
                    "\n" +
                    "   steps[gap+1] := true;\n" +
                    "\n" +
                    " END LOOP;\n" +
                    "\n" +
                    " return steps;\n" +
                    "END\n" +
                    "$$;";
    private static String V8_RETENTION_FUNCTIONS = "CREATE EXTENSION IF NOT EXISTS plv8;\n" +
            "create or replace function analyze_retention_intermediate(arr integer[], ff boolean[]) returns integer[] volatile language plv8 as $$\n" +
            "\n" +
            " if(ff == null) return arr;\n" +
            " for(var i=0; i <= ff.length; i++) {\n" +
            "   if(ff[i] === true) {\n" +
            "   arr[i] = arr[i] == null ? 1 : (arr[i]+1)\n" +
            "   }\n" +
            " }\n" +
            "\n" +
            " return arr;\n" +
            "$$;" +

            "create or replace function generate_timeline(start date, arr date[], durationmillis bigint, max_step integer) returns boolean[] volatile language plv8 as $$\n" +
            "\n" +
            " if(arr == null) {\n" +
            "  return null;\n" +
            " }\n" +
            " var steps = null;\n" +
            "\n" +
            " for(var i=0; i <= arr.length; i++) {\n" +
            "   var value = (arr[i]-start);\n" +
            "   if(value < 0) continue;\n" +
            "   \n" +
            "   var gap = value / durationmillis;\n" +
            "   if(gap > max_step) { \n" +
            "\tbreak; \n" +
            "   }\n" +
            "\n" +
            "   if(steps == null) {\n" +
            "       steps = new Array(Math.min(max_step, arr.length))\n" +
            "   }\n" +
            "\n" +
            "  //plv8.elog(ERROR, gap, value, start, durationmillis);\n" +
            "   steps[gap] = true;\n" +
            " }\n" +
            "\n" +
            " return steps;\n" +
            "$$;";
    private final PostgresqlQueryExecutor executor;
    private final Metastore metastore;
    private final ProjectConfig projectConfig;

    @Inject
    public PostgresqlRetentionQueryExecutor(ProjectConfig projectConfig, PostgresqlQueryExecutor executor, Metastore metastore) {
        this.executor = executor;
        this.metastore = metastore;
        this.projectConfig = projectConfig;
    }

    @PostConstruct
    public void setup() {
        try (Connection conn = executor.getConnection()) {
            try {
                conn.createStatement().execute(V8_RETENTION_FUNCTIONS);
            } catch (SQLException e) {
                // plv8 is not available, fallback to pl/pgsql
                conn.createStatement().execute(PL_PGGSQL_RETENTION_TIMELINE_FUNCTION);
                conn.createStatement().execute(PL_PGGSQL_RETENTION_AGGREGATE_FUNCTION);
            }

            try {
                conn.createStatement().execute("CREATE AGGREGATE collect_retention(boolean[])\n" +
                        "(\n" +
                        "    sfunc = analyze_retention_intermediate,\n" +
                        "    stype = integer[],\n" +
                        "    initcond = '{}'\n" +
                        ")");
            } catch (SQLException e) {
                if (!e.getSQLState().equals("42723")) {
                    throw Throwables.propagate(e);
                }
            }

            try {
                conn.createStatement().execute(String.format("CREATE TYPE retention_action AS " +
                                "(is_first boolean, %s timestamp)",
                        checkTableColumn(projectConfig.getTimeColumn())));
            } catch (SQLException e) {
                if (!e.getSQLState().equals("42710")) {
                    throw Throwables.propagate(e);
                }
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public QueryExecution query(RequestContext context, Optional<RetentionAction> firstAction,
                                Optional<RetentionAction> returningAction, DateUnit dateUnit,
                                Optional<String> dimension, Optional<Integer> period,
                                LocalDate startDate, LocalDate endDate, ZoneId zoneId, boolean approximate) {
        period.ifPresent(e -> checkArgument(e >= 0, "Period must be 0 or a positive value"));
        if (approximate) {
            // TODO: should we throw an exception or just show a warning?
//            throw new RakamException("Approximation is not supported.", HttpResponseStatus.BAD_REQUEST);
        }

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

        Optional<Integer> range = period.map(v ->
                Math.min(v, checkedCast(dateUnit.getTemporalUnit().between(start, end))));

        if (range.isPresent() && range.get() < 0) {
            throw new IllegalArgumentException("startDate must be before endDate.");
        }

        if (range.isPresent() && range.get() < 0) {
            return QueryExecution.completedQueryExecution(null, QueryResult.empty());
        }

        Map<String, List<SchemaField>> collections = metastore.getCollections(context.project);

        String firstActionQuery = generateQuery(
                collections, context.project, firstAction,
                testDeviceIdExists(firstAction, collections) ? format("coalesce(cast(%s as varchar), _device_id) as %s", projectConfig.getUserColumn(), checkTableColumn(projectConfig.getUserColumn())) : projectConfig.getUserColumn(),
                dimension, startDate, endDate);
        String returningActionQuery = generateQuery(
                collections, context.project, returningAction,
                testDeviceIdExists(firstAction, collections) ? format("coalesce(cast(%s as varchar), _device_id) as %s", projectConfig.getUserColumn(), checkTableColumn(projectConfig.getUserColumn())) : projectConfig.getUserColumn(),
                dimension, startDate, endDate);

        String query;
        if (firstAction.equals(returningAction)) {
            query = format("select %s, collect_retention(bits) from (\n" +
                            "select %s, (case when (not is_first %s) then \n" +
                            "generate_timeline(%s, timeline, %d::bigint, 15) else null end) as bits from (\n" +
                            "select %s %s, true as is_first, %s as timeline from (%s) t group by 1 %s " +
                            "UNION ALL " +
                            "select %s %s, false as is_first, array_agg(%s::date order by %s::date) as timeline from (%s) t group by 1 %s) t\n" +
                            "%s \n" +
                            ") t\n" +
                            "group by 1 order by 1 asc",
                    dimension.map(v -> "dimension").orElse("date"),
                    dimension.map(v -> "dimension").orElse("date"),
                    // do not check dimension value for first action dates.
                    dimension.map(v -> "").orElse("and dates.date = any(timeline)"),
                    dimension.map(v -> "timeline[1]").orElse("dates.date::date"),
                    dateUnit.getTemporalUnit().getDuration().toMillis(),

                    dimension.map(v -> "dimension").map(v -> v + ", ").orElse(""),
                    projectConfig.getUserColumn(),
                    // if we're calculating by dimension, take the first event data for each user
                    dimension.map(val -> format("array[min(%s)]", format(timeColumn, projectConfig.getTimeColumn())))
                            .orElseGet(() -> format("array_agg(%s::date order by %s::date)", format(timeColumn, projectConfig.getTimeColumn()), format(timeColumn, projectConfig.getTimeColumn()))),
                    firstActionQuery,
                    dimension.map(v -> ", 2").orElse(""),

                    dimension.map(v -> "dimension").map(v -> v + ", ").orElse(""),
                    projectConfig.getUserColumn(),
                    format(timeColumn, projectConfig.getTimeColumn()), format(timeColumn, projectConfig.getTimeColumn()),
                    returningActionQuery,
                    dimension.map(v -> ", 2").orElse(""),

                    dimension.map(v -> "").orElseGet(() -> String.format("cross join (select generate_series(date_trunc('%s', date '%s'), date_trunc('%s', date '%s'),  interval '1 %s')::date date) dates",
                            dateUnit.name().toLowerCase(ENGLISH), TIMESTAMP_FORMATTER.format(startDate.atStartOfDay()),
                            dateUnit.name().toLowerCase(ENGLISH), TIMESTAMP_FORMATTER.format(endDate.atStartOfDay()),
                            dateUnit.name().toLowerCase(ENGLISH))));
        } else {
            query = format("select %s, collect_retention(bits) from (\n" +
                            "select %s, (case when (%s) then \n" +
                            "generate_timeline(%s, ret.timeline, %d::bigint, 15) else null end) as bits from (\n" +
                            "select %s %s, %s as timeline from (%s) t group by 1 %s " +
                            ") first left join ( " +
                            "select %s %s, array_agg(%s::date order by %s::date) as timeline from (%s) t group by 1 %s \n" +
                            ") ret on (ret._user = first._user %s)\n" +
                            "%s \n" +
                            ") t\n" +
                            "group by 1 order by 1 asc",
                    dimension.map(v -> "dimension").orElse("date"),
                    dimension.map(v -> "first.dimension").orElse("date"),
                    // do not check dimension value for first action dates.
                    dimension.map(v -> "true").orElse("dates.date = any(ret.timeline)"),
                    dimension.map(v -> "first.timeline[1]").orElse("dates.date::date"),
                    dateUnit.getTemporalUnit().getDuration().toMillis(),

                    dimension.map(v -> "dimension").map(v -> v + ", ").orElse(""),
                    projectConfig.getUserColumn(),
                    // if we're calculating by dimension, take the first event data for each user
                    dimension.map(val -> format("array[min(%s)]", format(timeColumn, projectConfig.getTimeColumn())))
                            .orElseGet(() -> format("array_agg(%s::date order by %s::date)", format(timeColumn, projectConfig.getTimeColumn()), format(timeColumn, projectConfig.getTimeColumn()))),
                    firstActionQuery,
                    dimension.map(v -> ", 2").orElse(""),

                    dimension.map(v -> "dimension").map(v -> v + ", ").orElse(""),
                    projectConfig.getUserColumn(),
                    format(timeColumn, projectConfig.getTimeColumn()), format(timeColumn, projectConfig.getTimeColumn()),
                    returningActionQuery,
                    dimension.map(v -> ", 2").orElse(""),
                    dimension.map(v -> " and first.dimension = ret.dimension").orElse(""),
                    dimension.map(v -> "").orElseGet(() -> String.format("cross join (select generate_series(date_trunc('%s', date '%s'), date_trunc('%s', date '%s'),  interval '1 %s')::date date) dates",
                            dateUnit.name().toLowerCase(ENGLISH), TIMESTAMP_FORMATTER.format(startDate.atStartOfDay()),
                            dateUnit.name().toLowerCase(ENGLISH), TIMESTAMP_FORMATTER.format(endDate.atStartOfDay()),
                            dateUnit.name().toLowerCase(ENGLISH))));
        }

        return new DelegateQueryExecution(executor.executeRawQuery(context, query, zoneId), (result) -> {
            if (result.isFailed()) {
                return result;
            }
            ArrayList<List<Object>> rows = new ArrayList<>();
            for (List<Object> objects : result.getResult()) {
                Object date = objects.get(0);
                Integer[] days = (Integer[]) objects.get(1);
                for (int i = 0; i < days.length; i++) {
                    if (days[i] != null) {
                        rows.add(Arrays.asList(date, i == 0 ? null : ((long) i - 1), (long) days[i]));
                    }
                }
            }
            return new QueryResult(ImmutableList.of(
                    new SchemaField("dimension", result.getMetadata().get(0).getType()),
                    new SchemaField("lead", INTEGER),
                    new SchemaField("value", INTEGER)), rows, result.getProperties());
        });
    }

    private String generateQuery(
            Map<String, List<SchemaField>> collections,
            String project,
            Optional<RetentionAction> retentionAction,
            String connectorField,
            Optional<String> dimension,
            LocalDate startDate,
            LocalDate endDate) {
        String timePredicate = format("between timestamp '%s' and timestamp '%s' + interval '1' day",
                TIMESTAMP_FORMATTER.format(startDate.atStartOfDay()),
                TIMESTAMP_FORMATTER.format(endDate.atStartOfDay()));

        if (!retentionAction.isPresent()) {
            if (!collections.entrySet().stream().anyMatch(e -> e.getValue().stream().anyMatch(s -> s.getName().equals("_user")))) {
                return format("select %s, %s null as %s",
                        checkTableColumn(projectConfig.getTimeColumn()),
                        dimension.isPresent() ? checkTableColumn(dimension.get(), "dimension", '"') + " as dimension, " : "",
                        connectorField);
            }

            return collections.entrySet().stream()
                    .filter(entry -> entry.getValue().stream().anyMatch(e -> e.getName().equals("_user")))
                    .map(collection -> getTableSubQuery(project, collection.getKey(),
                            connectorField,
                            dimension, timePredicate, Optional.empty()))
                    .collect(Collectors.joining(" union all "));
        } else {
            String collection = retentionAction.get().collection();

            return getTableSubQuery(
                    project, collection,
                    connectorField,
                    dimension, timePredicate,
                    retentionAction.get().filter());
        }
    }

    protected String getTableSubQuery(String project, String collection,
                                      String connectorField,
                                      Optional<String> dimension,
                                      String timePredicate,
                                      Optional<Expression> filter) {
        return format("select %s, %s %s from %s where %s %s %s",
                checkTableColumn(projectConfig.getTimeColumn()),
                dimension.isPresent() ? checkTableColumn(dimension.get(), "dimension", '"') + " as dimension, " : "",
                connectorField,
                checkProject(project, '"') + "." + checkCollection(collection),
                checkTableColumn(projectConfig.getTimeColumn()),
                timePredicate,
                filter.isPresent() ? "and " + formatExpression(filter.get(), reference -> {
                    throw new UnsupportedOperationException();
                }, '"') : "");
    }
}