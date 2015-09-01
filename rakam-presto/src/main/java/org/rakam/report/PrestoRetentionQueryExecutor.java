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
package org.rakam.report;

import com.facebook.presto.hive.$internal.com.google.common.primitives.Ints;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.inject.Inject;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.collection.event.metastore.Metastore;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Boolean.valueOf;
import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/08/15 06:49.
 */
public class PrestoRetentionQueryExecutor implements RetentionQueryExecutor {
    private final int MAXIMUM_LEAD = 7;
    private final PrestoQueryExecutor executor;
    private final Metastore metastore;
    private final PrestoConfig config;

    @Inject
    public PrestoRetentionQueryExecutor(PrestoQueryExecutor executor, Metastore metastore, PrestoConfig config) {
        this.executor = executor;
        this.metastore = metastore;
        this.config = config;
    }

    @Override
    public QueryExecution query(String project,
                                Optional<RetentionAction> firstAction,
                                Optional<RetentionAction> returningAction,
                                DateUnit dateUnit,
                                Optional<String> dimension,
                                LocalDate startDate,
                                LocalDate endDate) {
        StringBuilder from = new StringBuilder();
        if(returningAction.isPresent()) {
            RetentionAction retentionAction = returningAction.get();
            from.append(generateQuery(project, retentionAction.collection(), dimension,
                    retentionAction.filter(),
                    firstAction.isPresent() ? Optional.of(false) : Optional.empty(),
                    startDate, endDate));
        } else {
            Set<String> collectionNames = metastore.getCollectionNames(project);
            from.append(collectionNames.stream()
                    .map(collection -> generateQuery(project, collection, dimension, Optional.empty(),
                            firstAction.map((action) -> false), startDate, endDate))
                    .collect(Collectors.joining(" union all ")));
        }
        if(firstAction.isPresent()) {
            RetentionAction retentionAction = firstAction.get();
            from.append(" union all ")
                .append(generateQuery(project, retentionAction.collection(), dimension,
                        retentionAction.filter(), Optional.of(true), startDate, endDate));
        }

        String dimensionColumn;
        String dimensionTransformation;
        String dimensionSubtraction;
        if(dimension.isPresent()) {
            dimensionColumn = dimension.get();
            dimensionTransformation = dimension.get();
            dimensionSubtraction = "lead-time_period";
        }else {
            if(dateUnit == DateUnit.DAY) {
                long seconds = ChronoUnit.DAYS.getDuration().getSeconds();
                dimensionColumn = format("time/%d", seconds);
                dimensionTransformation = format("cast(from_unixtime((time_period)*%d) as date)", seconds);
                dimensionSubtraction = "lead%d-time_period";
            } else
            if(dateUnit == DateUnit.WEEK) {
                long seconds = ChronoUnit.WEEKS.getDuration().getSeconds();
                dimensionColumn = format("date_trunc('week', from_unixtime(time))", seconds);
                dimensionTransformation ="time_period";
                dimensionSubtraction = "date_diff('week', time_period, lead%d)";
            } else
            if(dateUnit == DateUnit.MONTH) {
                long seconds = ChronoUnit.WEEKS.getDuration().getSeconds();
                dimensionColumn = format("date_trunc('month', from_unixtime(time))", seconds);
                dimensionTransformation ="time_period";
                dimensionSubtraction = "date_diff('month', time_period, lead%d)";
            } else {
                throw new UnsupportedOperationException();
            }
        }

        Period betweenDates = Period.between(startDate, endDate);
        int range = Ints.checkedCast(Math.min(betweenDates.get(dateUnit.getTemporalUnit()), MAXIMUM_LEAD));
        if(range <= 0) {
            throw new IllegalArgumentException("startDate and endDate are invalid.");
        }
        String leadTemplate = "lead(time_period, %d) over (partition by user order by user, time_period)";
        String leadColumns = IntStream.range(0, range)
                .mapToObj(i -> "(" + format(dimensionSubtraction, i) + ") as lead" + i)
                .collect(Collectors.joining(", "));
        String groups = IntStream.range(2, range+2).mapToObj(Integer::toString).collect(Collectors.joining(", "));
        String emptyColumns = IntStream.range(0, range).mapToObj(i -> "cast(null as bigint) as lead"+i).collect(Collectors.joining(", "));

        String query;
        if(firstAction.isPresent()) {
            String leads = IntStream.range(0, range)
                    .mapToObj(i -> "case when _first then " + format(leadTemplate, i) + " else null end lead" + i)
                    .collect(Collectors.joining(", "));

            query = format("with daily_groups as (\n" +
                            "  select user, (%s) as time_period, _first\n" +
                            "  from (%s) group by 1, 2, 3\n" +
                            "), \n" +
                            "lead_relations as (\n" +
                            "  select user, time_period, %s\n" +
                            "  from daily_groups\n" +
                            "),\n" +
                            "result as (\n" +
                            "   select %s as dimension, %s, count(distinct user) as count\n" +
                            "   from lead_relations where lead1 is not null group by 1, %s order by 1\n" +
                            ")",
                    dimensionColumn, from.toString(), leads, dimensionTransformation, leadColumns, groups);
        } else {
            String leads = IntStream.range(0, range)
                    .mapToObj(i -> format(leadTemplate+" lead"+i, i))
                    .collect(Collectors.joining(", "));

            query = format("with daily_groups as (\n" +
                            "  select user, (%s) as time_period\n" +
                            "  from (%s) group by 1, 2\n" +
                            "), \n" +
                            "lead_relations as (\n" +
                            "  select user, time_period, %s\n" +
                            "  from daily_groups\n" +
                            "),\n" +
                            "result as (\n" +
                            "   select %s as dimension, %s, count(distinct user) as count\n" +
                            "   from lead_relations group by 1, %s order by 1\n" +
                            ")",
                    dimensionColumn, from.toString(), leads, dimensionTransformation, leadColumns, groups);
        }

        String leadColumnNames = IntStream.range(0, range).mapToObj(i -> "lead"+i).collect(Collectors.joining(", "));

        query += format("\nselect %s as dimension, null as lead, count(user) as count from daily_groups group by 1\n" +
                "union all (select dimension, lead, count from result \n" +
               "CROSS JOIN unnest(array[%s]) t(lead) where lead is not null)",
                dimensionTransformation, leadColumnNames);
        return executor.executeRawQuery(query);
    }

    private static class TranformerQueryExecution implements QueryExecution {

        private final QueryExecution queryExecution;
        private final Function<QueryResult, QueryResult> transformer;

        public TranformerQueryExecution(QueryExecution queryExecution, Function<QueryResult, QueryResult> transformer) {
            this.queryExecution = queryExecution;
            this.transformer = transformer;
        }

        @Override
        public QueryStats currentStats() {
            return queryExecution.currentStats();
        }

        @Override
        public boolean isFinished() {
            return queryExecution.isFinished();
        }

        @Override
        public CompletableFuture<QueryResult> getResult() {
            return queryExecution.getResult().thenApply(transformer);
        }

        @Override
        public String getQuery() {
            return queryExecution.getQuery();
        }

        @Override
        public void kill() {
            queryExecution.kill();
        }
    }

    private String generateQuery(String project,
                                 String collection,
                                 Optional<String> dimension,
                                 Optional<Expression> exp,
                                 Optional<Boolean> firstAction,
                                 LocalDate startDate,
                                 LocalDate endDate) {
        ZoneId utc = ZoneId.of("UTC");

        long startTs = startDate.atStartOfDay().atZone(utc).toEpochSecond();
        long endTs = endDate.atStartOfDay().atZone(utc).toEpochSecond();

        return format("select user, %s" +
                        (firstAction.isPresent() ? ", " + valueOf(firstAction.get()).toString() + " as _first " : "") +
                        " from %s where time between %d and %d %s",
                dimension.isPresent() ? dimension.get() : "time",
                config.getColdStorageConnector() + "." + project +"."+collection,
                startTs, endTs,
                exp.isPresent() ? "and " + exp.get().accept(new ExpressionFormatter.Formatter(), false) : "");
    }
}
