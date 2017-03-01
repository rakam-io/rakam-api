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
package org.rakam.postgresql.report;

import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;

public class PostgresqlEventExplorer
        extends AbstractEventExplorer
{
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "lpad(cast(extract(hour FROM %s) as text), 2, '0')||':00'")
            .put(DAY_OF_MONTH, "extract(day FROM %s)||'th day'")
            .put(WEEK_OF_YEAR, "extract(doy FROM %s)||'th week'")
            .put(MONTH_OF_YEAR, "rtrim(to_char(%s, 'Month'))")
            .put(QUARTER_OF_YEAR, "extract(quarter FROM %s)||'th quarter'")
            .put(DAY_OF_WEEK, "rtrim(to_char(%s, 'Day'))")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as date)")
            .put(MONTH, "date_trunc('month', %s)")
            .put(YEAR, "date_trunc('year', %s)")
            .build();
    private final QueryExecutorService executorService;

    @Inject
    public PostgresqlEventExplorer(QueryExecutorService service, MaterializedViewService materializedViewService,
            ContinuousQueryService continuousQueryService)
    {
        super(service, materializedViewService, continuousQueryService, timestampMapping);
        this.executorService = service;
    }

    @Override
    public CompletableFuture<QueryResult> getEventStatistics(String project, Optional<Set<String>> collections, Optional<String> dimension, Instant startDate, Instant endDate)
    {
        checkProject(project);

        if (collections.isPresent() && collections.get().isEmpty()) {
            return CompletableFuture.completedFuture(QueryResult.empty());
        }

        if (dimension.isPresent()) {
            checkReference(timestampMapping, dimension.get(), startDate, endDate, collections.map(v -> v.size()).orElse(10));
        }

        String timePredicate = format("\"_time\" between timestamp '%s' and timestamp '%s' + interval '1' day",
                TIMESTAMP_FORMATTER.format(startDate), TIMESTAMP_FORMATTER.format(endDate));

        String collectionQuery = collections.map(v -> "(" + v.stream()
                .map(col -> String.format("SELECT _time, cast('%s' as text) as \"$collection\" FROM %s", col, checkCollection(col))).collect(Collectors.joining(" union all ")) + ") data")
                .orElse("_all");

        String query;
        if (dimension.isPresent()) {
            Optional<TimestampTransformation> aggregationMethod = fromPrettyName(dimension.get());
            if (!aggregationMethod.isPresent()) {
                throw new RakamException(BAD_REQUEST);
            }

            query = format("select \"$collection\", %s as %s, count(*) from %s where %s group by 1, 2 order by 2 desc",
                    format(timestampMapping.get(aggregationMethod.get()), "_time"),
                    aggregationMethod.get(), collectionQuery, timePredicate);
        }
        else {
            query = String.format("select \"$collection\", count(*) total \n" +
                    " from %s where %s group by 1", collectionQuery, timePredicate);
        }

        return executorService.executeQuery(project, query, Optional.empty(), "collection", 20000).getResult();
    }

    @Override
    public String convertSqlFunction(AggregationType aggType)
    {
        switch (aggType) {
            case AVERAGE:
                return "avg(%s)";
            case MAXIMUM:
                return "max(%s)";
            case MINIMUM:
                return "min(%s)";
            case COUNT:
                return "count(%s)";
            case SUM:
                return "sum(%s)";
            case COUNT_UNIQUE:
                return "count(distinct %s)";
            case APPROXIMATE_UNIQUE:
                return "count(distinct %s)";
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }

    @Override
    public String convertSqlFunction(AggregationType intermediate, AggregationType main)
    {
        String column = convertSqlFunction(intermediate);
        if (intermediate == AggregationType.SUM && main == AggregationType.COUNT) {
            return String.format("cast(%s as bigint)", column);
        }

        return column;
    }
}