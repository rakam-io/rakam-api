package org.rakam.report.eventexplorer;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.rakam.analysis.EventExplorer.ReferenceType.COLUMN;
import static org.rakam.analysis.EventExplorer.ReferenceType.REFERENCE;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.HOUR;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.fromString;
import static org.rakam.collection.SchemaField.stripName;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.util.DateTimeUtils.TIMESTAMP_FORMATTER;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public abstract class AbstractEventExplorer
        implements EventExplorer
{
    protected final static String TIME_INTERVAL_ERROR_MESSAGE = "Date interval is too big. Please narrow the date range or use different date dimension.";
    protected final Reference DEFAULT_SEGMENT = new Reference(COLUMN, "_collection");

    protected static SqlParser sqlParser = new SqlParser();
    private final QueryExecutorService executor;

    private final Map<TimestampTransformation, String> timestampMapping;
    private final MaterializedViewService materializedViewService;
    private final ContinuousQueryService continuousQueryService;

    public AbstractEventExplorer(QueryExecutorService executor,
            MaterializedViewService materializedViewService,
            ContinuousQueryService continuousQueryService,
            Map<TimestampTransformation, String> timestampMapping)
    {
        this.executor = executor;
        this.timestampMapping = timestampMapping;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
    }

    public static void checkReference(Map<TimestampTransformation, String> timestampMapping, String refValue, Instant startDate, Instant endDate, int size)
    {
        switch (fromString(refValue.replace(" ", "_"))) {
            case HOUR_OF_DAY:
            case DAY_OF_MONTH:
            case WEEK_OF_YEAR:
            case MONTH_OF_YEAR:
            case QUARTER_OF_YEAR:
            case DAY_OF_WEEK:
                return;
            case HOUR:
                if (startDate.atZone(ZoneOffset.UTC).until(endDate.atZone(ZoneOffset.UTC), ChronoUnit.HOURS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
            case DAY:
                if (startDate.atZone(ZoneOffset.UTC).until(endDate.atZone(ZoneOffset.UTC), DAYS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
            case MONTH:
                if (startDate.atZone(ZoneOffset.UTC).until(endDate.atZone(ZoneOffset.UTC), ChronoUnit.MONTHS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
            case YEAR:
                if (startDate.atZone(ZoneOffset.UTC).until(endDate.atZone(ZoneOffset.UTC), ChronoUnit.YEARS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
        }
    }

    public static String getColumnValue(Map<TimestampTransformation, String> timestampMapping, Reference ref, boolean format)
    {
        switch (ref.type) {
            case COLUMN:
                return format ? checkTableColumn(ref.value) : ref.value;
            case REFERENCE:
                return format(timestampMapping.get(fromString(ref.value.replace(" ", "_"))), "_time");
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    public static String getColumnReference(Reference ref)
    {
        switch (ref.type) {
            case COLUMN:
                return ref.value;
            case REFERENCE:
                return "_time";
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    private Optional<AggregationType> getIntermediateAggregation(AggregationType aggregationType)
    {
        switch (aggregationType) {
            case COUNT:
            case SUM:
                return Optional.of(AggregationType.SUM);
            case MINIMUM:
                return Optional.of(AggregationType.MINIMUM);
            case MAXIMUM:
                return Optional.of(AggregationType.MAXIMUM);
            default:
                return Optional.empty();
        }
    }

    @Override
    public QueryExecution analyze(
            String project,
            List<String> collections,
            Measure measure, Reference grouping,
            Reference segmentValue2,
            String filterExpression,
            Instant startDate,
            Instant endDate)
    {
        Reference segment = segmentValue2 == null ? DEFAULT_SEGMENT : segmentValue2;

        if (grouping != null && grouping.type == REFERENCE) {
            checkReference(timestampMapping, grouping.value, startDate, endDate, collections.size());
        }
        if (segment != null && segment.type == REFERENCE) {
            checkReference(timestampMapping, segment.value, startDate, endDate, collections.size());
        }

        Predicate<OLAPTable> groupedMetricsPredicate = options -> {
            Expression filterExp;
            if (filterExpression != null) {
                synchronized (sqlParser) {
                    filterExp = sqlParser.createExpression(filterExpression);
                }
            }
            else {
                filterExp = null;
            }

            if (options.collections.containsAll(collections)) {
                if (options.aggregations.contains(measure.aggregation)
                        && options.measures.contains(measure.column)
                        && (grouping == null || (grouping.type == REFERENCE || (grouping.type == COLUMN && options.dimensions.contains(grouping.value))))
                        && (segment == null || (segment.type == REFERENCE || (segment.type == COLUMN && options.dimensions.contains(segment.value))))
                        && (filterExp == null || testFilterExpressionForPerComputedTable(filterExp, options))) {
                    return true;
                }
            }

            return false;
        };

        Optional<Map.Entry<OLAPTable, String>> preComputedTable = materializedViewService.list(project).stream()
                .filter(view -> view.options != null && view.options.containsKey("olap_table"))
                .map(view -> JsonHelper.convert(view.options.get("olap_table"), OLAPTable.class))
                .filter(table -> groupedMetricsPredicate.test(table)).findAny()
                .map(view -> new AbstractMap.SimpleImmutableEntry<>(view, "materialized." + view.tableName));

        if (!preComputedTable.isPresent()) {
            preComputedTable = continuousQueryService.list(project).stream()
                    .filter(view -> view.options != null && view.options.containsKey("olap_table"))
                    .map(view -> JsonHelper.convert(view.options.get("olap_table"), OLAPTable.class))
                    .filter(table -> groupedMetricsPredicate.test(table)).findAny()
                    .map(view -> new AbstractMap.SimpleImmutableEntry<>(view, "continuous." + view.tableName));
        }

        String timeFilter = format(" _time between timestamp '%s' and timestamp '%s' + interval '1' day",
                TIMESTAMP_FORMATTER.format(startDate), TIMESTAMP_FORMATTER.format(endDate));

        String groupBy;
        if (segment != null && grouping != null) {
            groupBy = "GROUP BY 1, 2";
        }
        else if (segment != null || grouping != null) {
            groupBy = "GROUP BY 1";
        }
        else {
            groupBy = "";
        }

        String computeQuery;
        if (preComputedTable.isPresent()) {
            String filters = preComputedTable.get().getKey().dimensions.stream()
                    .filter(dim -> (grouping == null || grouping.type == REFERENCE && !grouping.value.equals(dim) &&
                            (segment == null || segment.type == REFERENCE && !segment.value.equals(dim))))
                    .map(dim -> String.format("%s is null", dim))
                    .collect(Collectors.joining(" and "));

            computeQuery = format("SELECT %s %s %s as value FROM %s WHERE %s %s",
                    grouping != null ? (getColumnValue(timestampMapping, grouping, true) + " as " + checkTableColumn(getColumnReference(grouping) + "_group") + " ,") : "",
                    segment != null ? (getColumnValue(timestampMapping, segment, true) + " as " + checkTableColumn(getColumnReference(segment) + "_segment") + " ,") : "",
                    format(getFinalForAggregationFunction(measure), measure.column + "_" + measure.aggregation.name().toLowerCase()),
                    checkCollection(preComputedTable.get().getValue()),
                    Stream.of(
                            collections.size() > 1 ? format("collection IN (%s)", collections.stream().map(c -> "'" + c + "'").collect(Collectors.joining(","))) : "",
                            filters,
                            filterExpression,
                            timeFilter
                    ).filter(e -> e != null && !e.isEmpty()).collect(Collectors.joining(" AND ")),
                    groupBy);
        }
        else {
            String where = timeFilter + (filterExpression == null ? "" : (" AND " + filterExpression));

            String measureAgg = convertSqlFunction(measure != null &&
                    measure.aggregation != null ? measure.aggregation : COUNT);
            String measureColumn = measure != null &&
                    measure.column != null ? checkTableColumn(measure.column) : "1";

            if (collections.size() == 1) {
                String select = generateComputeQuery(grouping, segment, collections.get(0));
                computeQuery = format("select %s %s as value from %s where %s %s",
                        select.isEmpty() ? select : select + ",",
                        format(measureAgg, measureColumn),
                        checkCollection(collections.get(0)),
                        where, groupBy);
            }
            else {
                String selectPart = (grouping == null ? "" : checkTableColumn(getColumnReference(grouping) + "_group")) +
                        (grouping == null ? "" : ", ") + checkTableColumn(getColumnReference(segment) + "_segment");

                String queries = collections.size() == 1 ? collections.get(0) : collections.stream()
                        .map(collection -> {
                            String select = generateComputeQuery(grouping, segment, collection);

                            String format = format("select %s %s from %s where %s",
                                    select.isEmpty() ? select : select + ",",
                                    measureColumn,
                                    checkCollection(collection), where);
                            return format;
                        })
                        .collect(Collectors.joining(" union all "));

                computeQuery = format("select %s %s as value from (%s) as data %s",
                        selectPart.isEmpty() ? "" : selectPart + ",",
                        format(measureAgg, measureColumn),
                        queries,
                        groupBy);
            }
        }

        String query = null;
        Optional<AggregationType> intermediateAggregation = getIntermediateAggregation(measure.aggregation);

        if (intermediateAggregation.isPresent()) {
            if (grouping != null) {
                if (grouping.type == COLUMN && segment.type == COLUMN) {
                    query = format(" SELECT " +
                                    " CASE WHEN group_rank > 15 THEN 'Others' ELSE cast(%s as varchar) END,\n" +
                                    " CASE WHEN segment_rank > 20 THEN 'Others' ELSE cast(%s as varchar) END,\n" +
                                    " %s FROM (\n" +
                                    "   SELECT *,\n" +
                                    "          row_number() OVER (ORDER BY %s DESC) AS group_rank,\n" +
                                    "          row_number() OVER (PARTITION BY %s ORDER BY value DESC) AS segment_rank\n" +
                                    "   FROM (%s) as data GROUP BY 1, 2, 3) as data GROUP BY 1, 2 ORDER BY 3 DESC",
                            checkTableColumn(getColumnReference(grouping) + "_group"),
                            checkTableColumn(getColumnReference(segment) + "_segment"),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            checkCollection(format(getColumnReference(grouping), "value") + "_group"),
                            computeQuery);
                }
            }
            else {
                String columnValue = null, suffix = null;
                boolean reference;

                if (segment != null) {
                    columnValue = getColumnValue(timestampMapping, segment, false);
                    reference = segment.type == REFERENCE;
                    suffix = "segment";
                }
                else if (grouping != null) {
                    columnValue = getColumnValue(timestampMapping, grouping, false);
                    reference = grouping.type == REFERENCE;
                    suffix = "group";
                }
                else {
                    reference = false;
                }

                if (columnValue != null && !reference) {
                    query = format(" SELECT " +
                                    " CASE WHEN group_rank > 50 THEN 'Others' ELSE CAST(%s as varchar) END, %s FROM (\n" +
                                    "   SELECT *, row_number() OVER (ORDER BY %s DESC) AS group_rank\n" +
                                    "   FROM (%s) as data GROUP BY 1, 2) as data GROUP BY 1 ORDER BY 2 DESC",
                            checkTableColumn(columnValue + "_" + suffix),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            computeQuery);
                }
                else {
                    query = computeQuery + " ORDER BY 1 DESC";
                }
            }
        }

        if (query == null) {
            query = format("select %s %s %s value from (%s) data ORDER BY %s DESC",
                    grouping == null ? "" : format(grouping.type == COLUMN ? "cast(" + checkTableColumn("%s_group") + " as varchar)" : checkTableColumn("%s_group"), getColumnReference(grouping)),
                    segment == null ? "" : ((grouping == null ? "" : ",") + format(segment.type == COLUMN ?
                            "cast(" + checkTableColumn("%s_segment") + " as varchar)" :
                            checkTableColumn("%s_segment"), getColumnReference(segment))),
                    grouping != null || segment != null ? "," : "",
                    computeQuery, segment != null && grouping != null ? 3 : 2);
        }

        String table = preComputedTable.map(e -> e.getValue()).orElse(null);

        return new DelegateQueryExecution(executor.executeQuery(project, query), result -> {
            if (table != null) {
                result.setProperty("olapTable", table);
            }
            return result;
        });
    }

    protected String generateComputeQuery(Reference grouping, Reference segment, String collection)
    {
        StringBuilder selectBuilder = new StringBuilder();
        if (grouping != null) {
            selectBuilder.append(getColumnValue(timestampMapping, grouping, true) + " as " + checkTableColumn(getColumnReference(grouping) + "_group"));
            if (segment != null) {
                selectBuilder.append(", ");
            }
        }
        if (segment != null) {
            selectBuilder.append((!segment.equals(DEFAULT_SEGMENT) ? getColumnValue(timestampMapping, segment, true) : "'" + stripName(collection) + "'") + " as "
                    + checkTableColumn(getColumnReference(segment) + "_segment"));
        }
        return selectBuilder.toString();
    }

    private boolean testFilterExpressionForPerComputedTable(Expression filterExp, OLAPTable options)
    {
        final boolean[] columnExists = {true};

        new DefaultExpressionTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context)
            {
                if (node.getName().getParts().size() != 1) {
                    columnExists[0] = false;
                }

                if (!options.dimensions.contains(node.getName().getParts().get(0))) {
                    columnExists[0] = false;
                }

                return null;
            }
        }.process(filterExp, null);

        return columnExists[0];
    }

    private String getFinalForAggregationFunction(Measure aggregation)
    {
        switch (aggregation.aggregation) {
            case AVERAGE:
                return "cast(sum(%1$s) as double) / count(%1$s)";
            case MAXIMUM:
                return "max(%s)";
            case MINIMUM:
                return "min(%s)";
            case COUNT:
                return "count(%s)";
            case SUM:
                return "sum(%s)";
            case COUNT_UNIQUE:
                throw new UnsupportedOperationException();
            case APPROXIMATE_UNIQUE:
                return getFinalForApproximateUniqueFunction();
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
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

        String timePredicate = format("\"week\" between cast(date_trunc('week', timestamp '%s') as date) and cast(date_trunc('week', timestamp '%s') as date) and \n" +
                        "\"_time\" between timestamp '%s' and timestamp '%s' + interval '1' day",
                TIMESTAMP_FORMATTER.format(startDate), TIMESTAMP_FORMATTER.format(endDate),
                TIMESTAMP_FORMATTER.format(startDate), TIMESTAMP_FORMATTER.format(endDate));

        String query;
        if (dimension.isPresent()) {
            Optional<TimestampTransformation> aggregationMethod = TimestampTransformation.fromPrettyName(dimension.get());
            if (!aggregationMethod.isPresent()) {
                throw new RakamException(BAD_REQUEST);
            }

            query = format("select collection, %s as %s, sum(total) from continuous._event_explorer_metrics where %s group by 1, 2 order by 2 desc",
                    aggregationMethod.get() == HOUR ? "_time" : format(timestampMapping.get(aggregationMethod.get()), "_time"),
                    aggregationMethod.get(), timePredicate);
        }
        else {
            query = String.format("select collection, coalesce(sum(total), 0) as total \n" +
                    " from continuous._event_explorer_metrics where %s group by 1", timePredicate);
        }

        return executor.executeQuery(project, query, 20000).getResult();
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

    public abstract String convertSqlFunction(AggregationType aggType);
}
