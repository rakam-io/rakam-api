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
import io.airlift.log.Logger;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

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
    private final ProjectConfig projectConfig;

    @Inject
    public PostgresqlEventExplorer(ProjectConfig projectConfig, QueryExecutorService service, MaterializedViewService materializedViewService,
            ContinuousQueryService continuousQueryService)
    {
        super(projectConfig, service, materializedViewService, continuousQueryService, timestampMapping);
        this.executorService = service;
        this.projectConfig = projectConfig;
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
            return format("cast(%s as bigint)", column);
        }

        return column;
    }

    @Override
    public CompletableFuture<QueryResult> getEventStatistics(String project, Optional<Set<String>> collections, Optional<String> dimension, LocalDate startDate, LocalDate endDate, ZoneId timezone)
    {
        CompletableFuture<QueryResult> eventStatistics = super.getEventStatistics(project, collections, dimension, startDate, endDate, timezone);
        return eventStatistics.thenApply(result -> {
//            if (!result.isFailed()) {
//                List<List<Object>> result1 = result.getResult();
//                if (dimension.isPresent() && TimestampTransformation.fromPrettyName(dimension.get()).get() == HOUR_OF_DAY) {
//                    ZoneOffset offset = timezone.getRules().getOffset(Instant.now());
//                    DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
//                            .appendValue(ChronoField.HOUR_OF_DAY, 2)
//                            .appendLiteral(':')
//                            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
//                            .toFormatter();
//
//                    for (List<Object> objects : result1) {
//                        String format = LocalTime.parse(objects.get(1).toString()).atOffset(UTC)
//                                .withOffsetSameInstant(offset)
//                                .format(dateTimeFormatter);
//                        objects.set(1, format);
//                    }
//                }
//            }

            return result;
        });
    }
}