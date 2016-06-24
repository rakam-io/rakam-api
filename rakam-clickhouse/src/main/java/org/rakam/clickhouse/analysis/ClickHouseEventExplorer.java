package org.rakam.clickhouse.analysis;

import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.time.LocalDate;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.DAY;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.DAY_OF_MONTH;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.DAY_OF_WEEK;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.HOUR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.HOUR_OF_DAY;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.MONTH;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.MONTH_OF_YEAR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.QUARTER_OF_YEAR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.WEEK_OF_YEAR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.YEAR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.fromPrettyName;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;

public class ClickHouseEventExplorer extends AbstractEventExplorer
{
    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "cast(extract(hour FROM %s) as UInt32)")
            .put(DAY_OF_MONTH, "cast(extract(day FROM %s) as UInt32)")
            .put(WEEK_OF_YEAR, "cast(extract(doy FROM %s) as UInt32)")
            .put(MONTH_OF_YEAR, "cast(extract(month FROM %s) as UInt32)")
            .put(QUARTER_OF_YEAR, "cast(extract(quarter FROM %s) as UInt32)")
            .put(DAY_OF_WEEK, "cast(extract(dow FROM %s) as UInt32)")
            .put(HOUR, "date_trunc('hour', %s)")
            .put(DAY, "cast(%s as Date)")
            .put(MONTH, "date_trunc('month', %s)")
            .put(YEAR, "date_trunc('year', %s)")
            .build();
    private final QueryExecutorService executorService;

    @Inject
    public ClickHouseEventExplorer(QueryExecutorService service, MaterializedViewService materializedViewService,
            ContinuousQueryService continuousQueryService) {
        super(service, materializedViewService, continuousQueryService, timestampMapping);
        this.executorService = service;
    }

    @Override
    public CompletableFuture<QueryResult> getEventStatistics(String project, Optional<Set<String>> collections, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        checkProject(project);

        if (collections.isPresent() && collections.get().isEmpty()) {
            return CompletableFuture.completedFuture(QueryResult.empty());
        }

        if (dimension.isPresent()) {
            checkReference(dimension.get(), startDate, endDate, collections.map(v -> v.size()).orElse(10));
        }

        String timePredicate = format("_time between cast(toDate('%s') as DateTime) and cast(toDate('%s') as DateTime)",
                startDate.format(ISO_DATE), endDate.plus(1, DAYS).format(ISO_DATE));

        String collectionQuery = collections.map(v -> "(" + v.stream()
                .map(col -> String.format("SELECT _time, cast('%s' as string) as \"$collection\" FROM %s", col, checkCollection(col, '\"'))).collect(Collectors.joining(", ")) + ") data")
                .orElse("_all");

        String query;
        if (dimension.isPresent()) {
            Optional<TimestampTransformation> aggregationMethod = fromPrettyName(dimension.get());
            if (!aggregationMethod.isPresent()) {
                throw new RakamException(BAD_REQUEST);
            }

            String function = format(timestampMapping.get(aggregationMethod.get()), "_time");
            query = format("select \"$collection\" as collection, %s as %s, count(*) from %s where %s group by \"$collection\", %s order by %s desc",
                    function,
                    aggregationMethod.get(), collectionQuery, timePredicate,
                    function, function);
        } else {
            query = String.format("select \"$collection\" as collection, count(*) total \n" +
                    " from %s where %s group by \"$collection\"", collectionQuery, timePredicate);
        }

        return executorService.executeQuery(project, query, 20000).getResult();
    }

    @Override
    public String convertSqlFunction(AggregationType aggType) {
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
                return "uniqExact(%s)";
            case APPROXIMATE_UNIQUE:
                return "uniq(distinct %s)";
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }
}
