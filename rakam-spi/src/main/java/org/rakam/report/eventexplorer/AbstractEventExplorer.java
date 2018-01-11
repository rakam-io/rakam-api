package org.rakam.report.eventexplorer;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.EventExplorerListener;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RequestContext;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.report.realtime.AggregationType;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;
import org.rakam.util.MaterializedViewNotExists;
import org.rakam.util.RakamException;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Optional.ofNullable;
import static org.rakam.analysis.EventExplorer.ReferenceType.COLUMN;
import static org.rakam.analysis.EventExplorer.ReferenceType.REFERENCE;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.fromString;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.report.realtime.AggregationType.SUM;
import static org.rakam.util.ValidationUtil.*;

public abstract class AbstractEventExplorer
        implements EventExplorer {
    protected final static String TIME_INTERVAL_ERROR_MESSAGE = "Date interval is too big. Please narrow the date range or use different date dimension.";
    private final static Logger LOGGER = Logger.get(AbstractEventExplorer.class);
    private final static String OTHERS_TAG = "Others";
    protected static SqlParser sqlParser = new SqlParser();
    protected final Reference DEFAULT_SEGMENT = new Reference(COLUMN, "_collection");
    private final QueryExecutorService executor;

    private final Map<TimestampTransformation, String> timestampMapping;
    private final MaterializedViewService materializedViewService;
    private final ProjectConfig projectConfig;

    public AbstractEventExplorer(
            ProjectConfig projectConfig,
            QueryExecutorService executor,
            MaterializedViewService materializedViewService,
            Map<TimestampTransformation, String> timestampMapping) {
        this.projectConfig = projectConfig;
        this.executor = executor;
        this.timestampMapping = timestampMapping;
        this.materializedViewService = materializedViewService;
    }

    public static void checkReference(String refValue, LocalDate startDate, LocalDate endDate, int size) {
        switch (fromString(refValue.replace(" ", "_"))) {
            case HOUR_OF_DAY:
            case DAY_OF_MONTH:
            case WEEK_OF_YEAR:
            case MONTH_OF_YEAR:
            case QUARTER_OF_YEAR:
            case DAY_OF_WEEK:
                return;
            case HOUR:
                if (startDate.atStartOfDay().until(endDate.plusDays(1).atStartOfDay(), ChronoUnit.HOURS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
            case DAY:
                if (startDate.atStartOfDay().until(endDate.plusDays(1).atStartOfDay(), DAYS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
            case MONTH:
                if (startDate.atStartOfDay().until(endDate.plusDays(1).atStartOfDay(), ChronoUnit.MONTHS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
            case YEAR:
                if (startDate.atStartOfDay().until(endDate.plusDays(1).atStartOfDay(), ChronoUnit.YEARS) > 30000 / size) {
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                }
                break;
        }
    }

    public CompletableFuture<PrecalculatedTable> create(RequestContext context, OLAPTable table) {
        String dimensions = table.dimensions.stream().map(d -> {
            if (d.type == null) {
                return d.value;
            }

            TimestampTransformation transformation = TimestampTransformation.fromString(d.type);
            return timestampMapping.get(transformation);
        }).collect(Collectors.joining(", "));

        String subQuery;
        String measures = table.measures.isEmpty() ? "" : (", " + table.measures.stream().collect(Collectors.joining(", ")));
        String dimension = dimensions.isEmpty() ? "" : ", " + dimensions;

        if (table.collections != null && !table.collections.isEmpty()) {
            subQuery = table.collections.stream().map(collection ->
                    format("SELECT cast('%s' as varchar) as _collection, %s %s %s FROM %s",
                            collection,
                            checkTableColumn(projectConfig.getTimeColumn()),
                            dimension,
                            measures, collection))
                    .collect(Collectors.joining(" UNION ALL "));
        } else {
            subQuery = format("SELECT _collection, %s %s FROM _all",
                    checkTableColumn(projectConfig.getTimeColumn()), dimension, measures);
        }

        String name = "Dimensions";

        String metrics = table.measures.stream().map(column -> table.aggregations.stream()
                .map(agg -> getAggregationColumn(agg, table.aggregations).map(e -> format(e, column) + " as " + column + "_" + agg.name().toLowerCase()))
                .filter(Optional::isPresent).map(Optional::get).collect(Collectors.joining(", ")))
                .collect(Collectors.joining(", "));
        if (metrics.isEmpty()) {
            metrics = "count(*)";
        }

        String query = format("SELECT _collection, _time %s %s FROM " +
                        "(SELECT _collection, CAST(%s AS DATE) as _time %s %s FROM (%s) data) data " +
                        "GROUP BY CUBE (_collection, _time %s) ORDER BY 1 ASC",
                dimension,
                ", " + metrics,
                checkTableColumn(projectConfig.getTimeColumn()),
                dimension,
                measures,
                subQuery,
                dimensions.isEmpty() ? "" : "," + dimensions);

        return materializedViewService.create(context, new MaterializedView(table.tableName, "Olap table", query,
                Duration.ofHours(1), null, null, ImmutableMap.of("olap_table", table)))
                .thenApply(v -> new PrecalculatedTable(name, table.tableName));
    }

    private Optional<String> getAggregationColumn(AggregationType agg, Set<AggregationType> aggregations) {
        switch (agg) {
            case AVERAGE:
                aggregations.add(COUNT);
                aggregations.add(SUM);
                return Optional.empty();
            case MAXIMUM:
                return Optional.of("max(%s)");
            case MINIMUM:
                return Optional.of("min(%s)");
            case COUNT:
                return Optional.of("count(%s)");
            case SUM:
                return Optional.of("sum(%s)");
            case COUNT_UNIQUE:
                throw new UnsupportedOperationException("Not supported yet.");
            case APPROXIMATE_UNIQUE:
                return Optional.of(getIntermediateForApproximateUniqueFunction());
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }

    public String getColumnValue(Map<TimestampTransformation, String> timestampMapping, Reference ref, boolean format) {
        switch (ref.type) {
            case COLUMN:
                return format ? checkTableColumn(ref.value) : ref.value;
            case REFERENCE:
                return format(timestampMapping.get(fromString(ref.value.replace(" ", "_"))), projectConfig.getTimeColumn());
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    public String getColumnReference(Reference ref) {
        switch (ref.type) {
            case COLUMN:
                return ref.value;
            case REFERENCE:
                return projectConfig.getTimeColumn();
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    private Optional<AggregationType> getIntermediateAggregation(AggregationType aggregationType) {
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

    protected QueryExecution analyzeInternal(
            RequestContext context,
            List<String> collections,
            Measure measure, Reference grouping,
            Reference segmentValue2,
            String filterExpression,
            LocalDate startDate,
            LocalDate endDate,
            ZoneId timezone,
            boolean summarise) {
        Reference segment = segmentValue2 == null ? DEFAULT_SEGMENT : segmentValue2;

        if (grouping != null && grouping.type == REFERENCE) {
            checkReference(grouping.value, startDate, endDate, collections.size());
        }
        if (segment != null && segment.type == REFERENCE) {
            checkReference(segment.value, startDate, endDate, collections.size());
        }

        Predicate<OLAPTable> groupedMetricsPredicate = options -> {
            Expression filterExp;
            if (filterExpression != null) {
                synchronized (sqlParser) {
                    filterExp = sqlParser.createExpression(filterExpression);
                }
            } else {
                filterExp = null;
            }

            if (options.collections.containsAll(collections)) {
                if (options.aggregations.contains(measure.aggregation)
                        && measure.column != null && options.measures.contains(measure.column)
                        && (grouping == null || (grouping.type == REFERENCE || (grouping.type == COLUMN && options.dimensions.contains(grouping.value))))
                        && (segment == null || (segment.value.equals("_collection") && segment.type == COLUMN && options.collections.size() == 1) || (segment.type == REFERENCE || (segment.type == COLUMN && options.dimensions.contains(segment.value))))
                        && (filterExp == null || testFilterExpressionForPerComputedTable(filterExp, options))) {
                    return true;
                }
            }

            return false;
        };

        Optional<Map.Entry<OLAPTable, String>> preComputedTable = materializedViewService.list(context.project).stream()
                .filter(view -> view.options != null && view.options.containsKey("olap_table"))
                .map(view -> {
                    try {
                        return JsonHelper.convert(view.options.get("olap_table"), OLAPTable.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(e -> e != null)
                .filter(table -> groupedMetricsPredicate.test(table)).findAny()
                .map(view -> new AbstractMap.SimpleImmutableEntry<>(view, "materialized." + checkCollection(view.tableName)));

        String timeFilter = format(" %s between date '%s' and date '%s' + interval '1' day",
                checkTableColumn(projectConfig.getTimeColumn()), startDate.format(ISO_DATE), endDate.format(ISO_DATE));

        String groupBy;
        boolean bothActive = segment != null && grouping != null;
        if (bothActive) {
            groupBy = "GROUP BY 1, 2";
        } else if (segment != null || grouping != null) {
            groupBy = "GROUP BY 1";
        } else {
            groupBy = "";
        }

        String computeQuery;
        if (preComputedTable.isPresent()) {
            String filters = preComputedTable.get().getKey().dimensions.stream()
                    .filter(dim -> (grouping == null || grouping.type == REFERENCE && !grouping.value.equals(dim) &&
                            (segment == null || segment.type == REFERENCE && !segment.value.equals(dim))))
                    .map(dim -> format("%s is null", dim))
                    .collect(Collectors.joining(" and "));

            computeQuery = format("SELECT %s %s %s as value FROM %s WHERE %s %s",
                    grouping != null ? (getColumnValue(timestampMapping, grouping, true) + " as " + checkTableColumn(getColumnReference(grouping) + "_group") + " ,") : "",
                    segment != null ? (getColumnValue(timestampMapping, segment, true) + " as " + checkTableColumn(getColumnReference(segment) + "_segment") + " ,") : "",
                    format(getFinalForAggregationFunction(measure), measure.column + "_" + measure.aggregation.name().toLowerCase()),
                    preComputedTable.get().getValue(),
                    Stream.of(
                            collections.size() > 1 ? format("collection IN (%s)", collections.stream().map(c -> "'" + c + "'").collect(Collectors.joining(","))) : "",
                            filters,
                            filterExpression,
                            timeFilter
                    ).filter(e -> e != null && !e.isEmpty()).collect(Collectors.joining(" AND ")),
                    groupBy);
        } else {
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
            } else {
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

        if (summarise && intermediateAggregation.isPresent()) {
            if (grouping != null && grouping.type == COLUMN && segment.type == COLUMN) {
                boolean segmentRanked = !segment.equals(DEFAULT_SEGMENT);
                query = format(" SELECT " +
                                " CASE WHEN group_rank > 15 THEN '%s' ELSE cast(%s as varchar) END,\n" +
                                " %s " +
                                " %s FROM (\n" +
                                "   SELECT *,\n" +
                                "          %s" +
                                "          row_number() OVER (ORDER BY %s DESC) AS group_rank\n" +
                                "   FROM (%s) as data GROUP BY 1, 2, 3) as data GROUP BY 1, 2 ORDER BY 3 DESC",
                        OTHERS_TAG,
                        checkTableColumn(getColumnReference(grouping) + "_group"),
                        format(segmentRanked ? (" CASE WHEN segment_rank > 20 THEN '" + OTHERS_TAG + "' ELSE cast(%s as varchar) END,\n") : "cast(%s as varchar),", checkTableColumn(getColumnReference(segment) + "_segment")),
                        format(convertSqlFunction(intermediateAggregation.get(), measure.aggregation), "value"),
                        segmentRanked ? format("row_number() OVER (PARTITION BY %s ORDER BY value DESC) AS segment_rank,\n", checkCollection(format(getColumnReference(grouping), "value") + "_group")) : "",
                        format(convertSqlFunction(intermediateAggregation.get(), measure.aggregation), "value"),
                        computeQuery);
            } else {
                if ((grouping != null && grouping.type == COLUMN) || (segment != null && segment.type == COLUMN && !segment.value.equals("_collection"))) {
                    String windowColumn = checkTableColumn(getColumnValue(timestampMapping,
                            (grouping != null && grouping.type == COLUMN) ? grouping : segment, false) +
                            ((grouping != null && grouping.type == COLUMN) ? "_group" : "_segment"));

                    query = format(" SELECT " +
                                    " %s CASE WHEN group_rank > 50 THEN '%s' ELSE CAST(%s as varchar) END, %s FROM (\n" +
                                    "   SELECT *, row_number() OVER (ORDER BY %s DESC) AS group_rank\n" +
                                    "   FROM (%s) as data GROUP BY 1, 2 %s) as data GROUP BY 1 %s ORDER BY %d DESC",
                            bothActive ? checkTableColumn(getColumnReference(grouping.type == COLUMN ? segment : grouping) + (grouping.type == COLUMN ? "_segment" : "_group")) + ", " : "",
                            OTHERS_TAG,
                            windowColumn,
                            format(convertSqlFunction(intermediateAggregation.get(), measure.aggregation), "value"),
                            format(convertSqlFunction(intermediateAggregation.get(), measure.aggregation), "value"),
                            computeQuery, bothActive ? ", 3" : "", bothActive ? ", 2" : "", bothActive ? 3 : 2);
                } else {
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
                    computeQuery, bothActive ? 3 : 2);
        }

        String table = preComputedTable.map(e -> e.getValue()).orElse(null);

        return new DelegateQueryExecution(executor.executeQuery(context, query, timezone), result -> {
            if (table != null) {
                result.setProperty("olapTable", table);
            }
            if (result.isFailed()) {
                RuntimeException exception = new RuntimeException("Error while running event explorer query", new RuntimeException(result.getError().message,
                        new RuntimeException(ofNullable(result.getProperties().get("query")).map(Object::toString).orElse("Query could not found"))));
                LOGGER.error(exception);
            } else {
                boolean grouped = false;
                for (List<Object> objects : result.getResult()) {
                    Object dimensionVal = objects.get(0);
                    Object segmentVal = objects.get(1);
                    if (OTHERS_TAG.equals(dimensionVal) || OTHERS_TAG.equals(segmentVal)) {
                        grouped = true;
                        break;
                    }
                }
                result.setProperty("grouped", grouped);
            }
            return result;
        });
    }

    @Override
    public QueryExecution analyze(
            RequestContext context,
            List<String> collections,
            Measure measure, Reference grouping,
            Reference segmentValue2,
            String filterExpression,
            LocalDate startDate,
            LocalDate endDate,
            ZoneId timezone) {
        return analyzeInternal(context, collections, measure, grouping, segmentValue2, filterExpression, startDate, endDate, timezone, true);
    }

    @Override
    public QueryExecution export(
            RequestContext context,
            List<String> collections,
            Measure measure, Reference grouping,
            Reference segmentValue2,
            String filterExpression,
            LocalDate startDate,
            LocalDate endDate,
            ZoneId timezone) {
        return analyzeInternal(context, collections, measure, grouping, segmentValue2, filterExpression, startDate, endDate, timezone, false);
    }

    protected String generateComputeQuery(Reference grouping, Reference segment, String collection) {
        StringBuilder selectBuilder = new StringBuilder();
        if (grouping != null) {
            selectBuilder.append(getColumnValue(timestampMapping, grouping, true) + " as " + checkTableColumn(getColumnReference(grouping) + "_group"));
            if (segment != null) {
                selectBuilder.append(", ");
            }
        }
        if (segment != null) {
            selectBuilder.append((!segment.equals(DEFAULT_SEGMENT) ? getColumnValue(timestampMapping, segment, true) : "'" + checkLiteral(collection) + "'") + " as "
                    + checkTableColumn(getColumnReference(segment) + "_segment"));
        }
        return selectBuilder.toString();
    }

    private boolean testFilterExpressionForPerComputedTable(Expression filterExp, OLAPTable options) {
        final boolean[] columnExists = {true};

        new DefaultExpressionTraversalVisitor<Void, Void>() {
            @Override
            protected Void visitIdentifier(Identifier node, Void context) {
                if (!options.dimensions.contains(node.getValue())) {
                    columnExists[0] = false;
                }

                return null;
            }
        }.process(filterExp, null);

        return columnExists[0];
    }

    private String getFinalForAggregationFunction(Measure aggregation) {
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
    public CompletableFuture<QueryResult> getEventStatistics(RequestContext context,
                                                             Optional<Set<String>> collections,
                                                             Optional<String> dimension,
                                                             LocalDate startDate,
                                                             LocalDate endDate,
                                                             ZoneId timezone) {
        if (collections.isPresent() && collections.get().isEmpty()) {
            return CompletableFuture.completedFuture(QueryResult.empty());
        }

        if (dimension.isPresent()) {
            checkReference(dimension.get(), startDate, endDate, collections.map(v -> v.size()).orElse(10));
        }

        String timePredicate = format("_time between date '%s' and date '%s' + interval '1' day",
                startDate.format(ISO_DATE), endDate.format(ISO_DATE));

        String query;
        if (dimension.isPresent()) {
            Optional<TimestampTransformation> aggregationMethod = TimestampTransformation.fromPrettyName(dimension.get());
            if (!aggregationMethod.isPresent()) {
                throw new RakamException(BAD_REQUEST);
            }

            TimestampTransformation timestampTransformation = aggregationMethod.get();

            query = format("select collection, %s as %s, cast(sum(total) as bigint) from (%s) data where %s group by 1, 2 order by 2 desc",
                    format(timestampMapping.get(timestampTransformation), "_time"),
                    aggregationMethod.get(),
                    sourceTable(collections),
                    timePredicate);
        } else {
            query = format("select collection, cast(coalesce(sum(total), 0) as bigint) as total \n" +
                    " from (%s) data where %s group by 1", sourceTable(collections), timePredicate);
        }

        QueryExecution execution;
        try {
            execution = executor.executeQuery(context, query, Optional.empty(), null, timezone, 20000);
        } catch (MaterializedViewNotExists e) {
            new EventExplorerListener(projectConfig, materializedViewService).createTable(context.project);
            execution = executor.executeQuery(context, query, Optional.empty(), null, timezone, 20000);
        }

        return execution.getResult().thenApply(result -> {
            if (result.isFailed()) {
                LOGGER.error(new RuntimeException(result.getError().toString()),
                        "An error occurred while executing event explorer statistics query.");
            }

            return result;
        });
    }

    public String sourceTable(Optional<Set<String>> collections) {
        String predicate = collections.map(e -> "WHERE _collection IN (" +
                e.stream().map(n -> "'" + checkLiteral(n) + "'").collect(Collectors.joining(", ")) + ")").orElse("");
        return format("select _time, total, _collection as collection from materialized.%s %s",
                EventExplorerListener.tableName(), predicate);
    }

    @Override
    public Map<String, List<String>> getExtraDimensions(String project) {
        Map<String, List<String>> builder = new HashMap<>();
        for (TimestampTransformation transformation : timestampMapping.keySet()) {
            builder.computeIfAbsent(transformation.getCategory(), k -> new ArrayList<>())
                    .add(transformation.getPrettyName());
        }
        return builder;
    }

    public abstract String convertSqlFunction(AggregationType aggType);

    public String convertSqlFunction(AggregationType intermediate, AggregationType main) {
        return convertSqlFunction(intermediate);
    }

    public static class PrecalculatedTable {
        public final String name;
        public final String tableName;

        @JsonCreator
        public PrecalculatedTable(@ApiParam("name") String name, @ApiParam("tableName") String tableName) {
            this.name = name;
            this.tableName = tableName;
        }
    }
}
