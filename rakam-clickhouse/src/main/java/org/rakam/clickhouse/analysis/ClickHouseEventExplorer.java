package org.rakam.clickhouse.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.EventExplorer;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamExpressionFormatter.formatIdentifier;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.rakam.analysis.EventExplorer.ReferenceType.COLUMN;
import static org.rakam.analysis.EventExplorer.ReferenceType.REFERENCE;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.DAY;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.DAY_OF_MONTH;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.DAY_OF_WEEK;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.HOUR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.HOUR_OF_DAY;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.MONTH;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.MONTH_OF_YEAR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.YEAR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.fromPrettyName;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.fromString;
import static org.rakam.clickhouse.analysis.ClickHouseQueryExecution.DATE_TIME_FORMATTER;
import static org.rakam.collection.SchemaField.stripName;
import static org.rakam.report.eventexplorer.AbstractEventExplorer.checkReference;
import static org.rakam.report.eventexplorer.AbstractEventExplorer.getColumnReference;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkLiteral;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class ClickHouseEventExplorer
        implements EventExplorer
{
    protected final Reference DEFAULT_SEGMENT = new Reference(COLUMN, "_collection");

    private static final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "toHour(%s)")
            .put(DAY_OF_MONTH, "toDayOfMonth(%s)")
//            .put(WEEK_OF_YEAR, "cast(extract(doy FROM %s) as UInt32)")
            .put(MONTH_OF_YEAR, "toMonth(%s)")
//            .put(QUARTER_OF_YEAR, "cast(extract(quarter FROM %s) as UInt32)")
            .put(DAY_OF_WEEK, "toDayOfWeek(%s)")
            .put(HOUR, "toStartOfHour(%s)")
            .put(DAY, "cast(%s as Date)")
            .put(MONTH, "toStartOfMonth(%s)")
            .put(YEAR, "toStartOfYear(%s)")
            .build();
    private final QueryExecutor executor;
    private final QueryExecutorService service;
    private static final SqlParser sqlParser = new SqlParser();

    @Inject
    public ClickHouseEventExplorer(QueryExecutor executor, QueryExecutorService service)
    {
        this.executor = executor;
        this.service = service;
    }

    @Override
    public QueryExecution analyze(String project, List<String> collections, Measure measure, Reference grouping,
            Reference segmentValue2, String filterExpression, Instant startDate, Instant endDate)
    {
        Reference segment = segmentValue2 == null ? DEFAULT_SEGMENT : segmentValue2;

        if (grouping != null && grouping.type == REFERENCE) {
            checkReference(timestampMapping, grouping.value, startDate, endDate, collections.size());
        }
        if (segment != null && segment.type == REFERENCE) {
            checkReference(timestampMapping, segment.value, startDate, endDate, collections.size());
        }

        String groups = Arrays.asList(
                new AbstractMap.SimpleEntry<>("segment", segment),
                new AbstractMap.SimpleEntry<>("group", grouping)).stream()
                .filter(e -> e != null)
                .map(e -> getColumnReference(e.getValue()) + "_" + e.getKey())
                .collect(Collectors.joining(", "));
        String groupBy = groups.isEmpty() ? "" : ("GROUP BY " + groups + " WITH TOTALS");

        String timeFilter = format(" _time between toDateTime('%s') and toDateTime('%s')",
                DATE_TIME_FORMATTER.format(startDate), DATE_TIME_FORMATTER.format(endDate.plus(1, DAYS)));

        if (filterExpression != null) {
            synchronized (sqlParser) {
                Expression expression = sqlParser.createExpression(filterExpression);
                filterExpression = formatExpression(expression);
            }
        }

        String where = timeFilter + (filterExpression == null ? "" : (" AND " + filterExpression));

        String measureAgg = convertSqlFunction(measure != null &&
                measure.aggregation != null ? measure.aggregation : COUNT);
        String measureColumn = measure != null &&
                measure.column != null ? checkTableColumn(measure.column, '`') : "";

        String computeQuery;
        if (collections.size() == 1) {
            String select = generateComputeQuery(grouping, segment, collections.get(0));
            computeQuery = format("    select %s %s as value from %s.%s where %s %s",
                    select.isEmpty() ? select : select + ",",
                    format(measureAgg, measureColumn),
                    project, checkCollection(collections.get(0), '`'),
                    where, groupBy);
        }
        else {
            String selectPart = (grouping == null ? "" : checkTableColumn(getColumnReference(grouping) + "_group", '`')) +
                    (grouping == null ? "" : ", ") + checkTableColumn(getColumnReference(segment) + "_segment", '`');

            String queries = collections.size() == 1 ? collections.get(0) : collections.stream()
                    .map(collection -> {
                        String select = generateComputeQuery(grouping, segment, collection);

                        String format = format("select '%s' as _collection, %s %s from %s.%s where %s",
                                checkLiteral(collection),
                                measureColumn.isEmpty() ? select : measureColumn + ",",
                                measureColumn,
                                project, checkCollection(collection, '`'), where);
                        return format;
                    })
                    .collect(Collectors.joining("\n union all "));

            computeQuery = format("select %s %s as value from (\n" +
                            "%s\n" +
                            ") %s",
                    selectPart.isEmpty() ? "" : selectPart + ",",
                    format(measureAgg, measureColumn),
                    queries,
                    groupBy);
        }

        String query = format("select %s %s %s value from (\n" +
                        "%s\n" +
                        ") ORDER BY %s DESC LIMIT 500",
                grouping == null ? "" : format(grouping.type == COLUMN ? checkTableColumn("%s_group", '`') : checkTableColumn("%s_group", '`'), getColumnReference(grouping)),
                segment == null ? "" : ((grouping == null ? "" : ",") + format(segment.type == COLUMN ?
                        checkTableColumn("%s_segment", '`') :
                        checkTableColumn("%s_segment", '`'), getColumnReference(segment))),
                grouping != null || segment != null ? "," : "",
                computeQuery, segment != null && grouping != null ? 3 : 2);

        return new DelegateQueryExecution(executor.executeRawQuery(query), result -> {
            List<List<Object>> newResult = result.getResult();
            return new QueryResult(result.getMetadata(), newResult, result.getProperties());
        });
    }

    private static String formatExpression(Expression value)
    {
        return ClickhouseExpressionFormatter.formatExpression(value,
                name -> name.getParts().stream().map(e -> formatIdentifier(e, '`')).collect(Collectors.joining(".")),
                name -> name.getParts().stream()
                        .map(e -> formatIdentifier(e, '`')).collect(Collectors.joining(".")), '`');
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

        String timePredicate = format("_time between toDateTime('%s') and toDateTime('%s')",
                DATE_TIME_FORMATTER.format(startDate),
                DATE_TIME_FORMATTER.format(endDate.plus(1, DAYS)));

        String collectionQuery = collections.map(v -> "(" + v.stream()
                .map(col -> String.format("SELECT _time, cast('%s' as string) as \"$collection\" FROM %s",
                        col, checkCollection(col, '`'))).collect(Collectors.joining(", ")) + ") ")
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
        }
        else {
            query = String.format("select \"$collection\" as collection, count(*) total \n" +
                    " from %s where %s group by \"$collection\"", collectionQuery, timePredicate);
        }

        return service.executeQuery(project, query).getResult();
    }

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
                return "uniqExact(%s)";
            case APPROXIMATE_UNIQUE:
                return "uniq(distinct %s)";
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }

    protected String generateComputeQuery(Reference grouping, Reference segment, String collection)
    {
        StringBuilder selectBuilder = new StringBuilder();
        if (grouping != null) {
            selectBuilder.append(getColumnValue(grouping, true) + " as " + checkTableColumn(getColumnReference(grouping) + "_group", '`'));
            if (segment != null) {
                selectBuilder.append(", ");
            }
        }
        if (segment != null) {
            selectBuilder.append((!segment.equals(DEFAULT_SEGMENT) ? getColumnValue(segment, true) : "'" + stripName(collection, "collection") + "'") + " as "
                    + checkTableColumn(getColumnReference(segment) + "_segment", '`'));
        }
        return selectBuilder.toString();
    }

    protected String getColumnValue(Reference ref, boolean format)
    {
        switch (ref.type) {
            case COLUMN:
                return format ? checkTableColumn(ref.value, '`') : ref.value;
            case REFERENCE:
                return format(timestampMapping.get(fromString(ref.value.replace(" ", "_"))), "_time");
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    @Override
    public Map<String, List<String>> getExtraDimensions(String project)
    {
        Map<String, List<String>> builder = new HashMap<>();
        for (TimestampTransformation transformation : timestampMapping.keySet()) {
            builder.computeIfAbsent(transformation.getCategory(), k -> new ArrayList<>())
                    .add(transformation.getPrettyName());
        }
        return builder;
    }
}
