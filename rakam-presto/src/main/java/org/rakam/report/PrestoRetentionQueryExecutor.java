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

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.google.inject.Inject;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.collection.event.metastore.Metastore;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Boolean.valueOf;
import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/08/15 06:49.
 */
public class PrestoRetentionQueryExecutor implements RetentionQueryExecutor {

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
                    retentionAction.filter(), Optional.of(false), startDate, endDate));
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
                dimensionSubtraction = "lead-time_period";
            } else
            if(dateUnit == DateUnit.WEEK) {
                long seconds = ChronoUnit.WEEKS.getDuration().getSeconds();
                dimensionColumn = format("date_trunc('week', from_unixtime(time))", seconds);
                dimensionTransformation ="time_period";
                dimensionSubtraction = "date_diff('week', time_period, lead)";
            } else
            if(dateUnit == DateUnit.MONTH) {
                long seconds = ChronoUnit.WEEKS.getDuration().getSeconds();
                dimensionColumn = format("date_trunc('month', from_unixtime(time))", seconds);
                dimensionTransformation ="time_period";
                dimensionSubtraction = "date_diff('month', time_period, lead)";
            } else {
                throw new UnsupportedOperationException();
            }
        }



        String query;
        if(firstAction.isPresent()) {
            query = format("with daily_groups as (\n" +
                            "  select user, (%s) as time_period, _first\n" +
                            "  from (%s) group by 1, 2, 3\n" +
                            "), \n" +
                            "lead_relations as (\n" +
                            "  select user, time_period, case when _first then lead(time_period, 1) over (partition by user order by user, time_period) else -1 end lead\n" +
                            "  from daily_groups\n" +
                            ")\n" +
                            "select %s as date, null as day, count(user) from daily_groups where _first group by 1 \n" +
                            "union all\n" +
                            "select %s as dimension, (case when lead is null then 0 else (%s) end) as day, count(distinct user) as count\n" +
                            "from lead_relations where lead != -1 group by 1,2 order by 1, 2",
                    dimensionColumn, from.toString(), dimensionTransformation, dimensionTransformation, dimensionSubtraction);
        } else {
            query = format("with daily_groups as (\n" +
                            "  select user, (%s) as time_period\n" +
                            "  from (%s) group by 1, 2\n" +
                            "), \n" +
                            "lead_relations as (\n" +
                            "  select user, time_period, lead(time_period, 1) over (partition by user order by user, time_period) lead\n" +
                            "  from daily_groups\n" +
                            ")\n" +
                            "select %s as date, null as day, count(user) as count from daily_groups group by 1 \n" +
                            "union all\n" +
                            "select %s as dimension, (case when lead is null then 0 else (%s) end) as day, count(distinct user) as count" +
                            "from lead_relations group by 1,2 order by 1, 2",
                    dimensionColumn, from.toString(), dimensionTransformation, dimensionTransformation, dimensionSubtraction);
        }
        return executor.executeRawQuery(query);
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
