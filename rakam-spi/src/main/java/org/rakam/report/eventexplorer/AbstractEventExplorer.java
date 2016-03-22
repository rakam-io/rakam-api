package org.rakam.report.eventexplorer;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionUtil;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
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
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkProject;

public abstract class AbstractEventExplorer implements EventExplorer {
    private final static String TIME_INTERVAL_ERROR_MESSAGE = "Date interval is too big. Please narrow the date range or use different date dimension.";
    private static SqlParser sqlParser = new SqlParser();
    private final QueryExecutorService executor;

    private final Metastore metastore;
    private final Map<EventExplorer.TimestampTransformation, String> timestampMapping;
    private final MaterializedViewService materializedViewService;
    private final ContinuousQueryService continuousQueryService;

    public AbstractEventExplorer(QueryExecutorService executor,
                                 MaterializedViewService materializedViewService,
                                 ContinuousQueryService continuousQueryService,
                                 Metastore metastore,
                                 Map<TimestampTransformation, String> timestampMapping) {
        this.executor = executor;
        this.metastore = metastore;
        this.timestampMapping = timestampMapping;
        this.materializedViewService = materializedViewService;
        this.continuousQueryService = continuousQueryService;
    }

    private void checkReference(String refValue, LocalDate startDate, LocalDate endDate, int size) {
        switch (fromString(refValue.replace(" ", "_"))) {
            case HOUR_OF_DAY:
            case DAY_OF_MONTH:
            case WEEK_OF_YEAR:
            case MONTH_OF_YEAR:
            case QUARTER_OF_YEAR:
            case DAY_OF_WEEK:
                return;
            case HOUR:
                if (startDate.atStartOfDay().until(endDate.plus(1, DAYS).atStartOfDay(), ChronoUnit.HOURS) > 20000 / size)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                break;
            case DAY:
                if (startDate.until(endDate.plus(1, DAYS), DAYS) > 20000 / size)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                break;
            case MONTH:
                if (startDate.until(endDate.plus(1, DAYS), ChronoUnit.MONTHS) > 20000 / size)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                break;
            case YEAR:
                if (startDate.until(endDate.plus(1, DAYS), ChronoUnit.YEARS) > 20000 / size)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, BAD_REQUEST);
                break;
        }

    }

    private String getColumnValue(Reference ref) {
        switch (ref.type) {
            case COLUMN:
                return ref.value;
            case REFERENCE:
                return format(timestampMapping.get(fromString(ref.value.replace(" ", "_"))), "_time");
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    private String getColumnReference(Reference ref) {
        switch (ref.type) {
            case COLUMN:
                return ref.value;
            case REFERENCE:
                return "_time";
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

    @Override
    public QueryExecution analyze(String project, List<String> collections, Measure measureType, Reference grouping,
                                  Reference segment, String filterExpression, LocalDate startDate, LocalDate endDate) {
        if (grouping != null && grouping.type == REFERENCE) {
            checkReference(grouping.value, startDate, endDate, collections.size());
        }
        if (segment != null && segment.type == REFERENCE) {
            checkReference(segment.value, startDate, endDate, collections.size());
        }

        Expression filterExp;
        if (filterExpression != null) {
            synchronized (sqlParser) {
                filterExp = sqlParser.createExpression(filterExpression);
            }
        } else {
            filterExp = null;
        }

        Predicate<OLAPTable> groupedMetricsPredicate = options -> {
            if (options.collections.containsAll(collections)) {
                if (options.aggregations.contains(measureType.aggregation)
                        && options.measures.contains(measureType.column)
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

        StringBuilder selectBuilder = new StringBuilder();
        if (grouping != null) {
            selectBuilder.append(getColumnValue(grouping) + " as " + getColumnReference(grouping) + "_group");
            if (segment != null) {
                selectBuilder.append(", ");
            }
        }
        if (segment != null) {
            selectBuilder.append(getColumnValue(segment) + " as " + getColumnReference(segment) + "_segment");
        }
        String select = selectBuilder.toString();

        String timeFilter = format(" _time between date '%s' and date '%s' + interval '1' day",
                startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE));

        String groupBy;
        if (segment != null && grouping != null) {
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
                    .map(dim -> String.format("%s is null", dim))
                    .collect(Collectors.joining(" and "));

            computeQuery = format("SELECT %s %s %s as value FROM %s WHERE %s %s",
                    grouping != null ? (getColumnValue(grouping) + " as " + getColumnReference(grouping) + "_group ,") : "",
                    segment != null ? (getColumnValue(segment) + " as " + getColumnReference(segment) + "_segment ,") : "",
                    format(getFinalForAggregationFunction(measureType), measureType.column + "_" + measureType.aggregation.name().toLowerCase()),
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

            String measureAgg = convertSqlFunction(measureType != null &&
                    measureType.aggregation != null ? measureType.aggregation : COUNT);
            String measureColumn = measureType != null &&
                    measureType.column != null ? measureType.column : "*";

            if (collections.size() == 1) {
                computeQuery = format("select %s %s as value from %s where %s %s",
                        select.isEmpty() ? select : select + ",",
                        format(measureAgg, measureColumn),
                        collections.get(0),
                        where, groupBy);
            } else {
                String selectPart = (grouping == null ? "" : getColumnReference(grouping) + "_group") +
                        (segment == null ? "" :
                                (grouping == null ? "" : ", ") + getColumnReference(segment) + "_segment");

                String queries = collections.size() == 1 ? collections.get(0) : collections.stream()
                        .map(collection -> format("select %s %s from %s where %s",
                                select.isEmpty() ? select : select + ",",
                                measureColumn,
                                QualifiedName.of(collection), where))
                        .collect(Collectors.joining(" union all "));

                computeQuery = format("select %s %s as value from (%s) as data %s",
                        select.isEmpty() ? "" : selectPart + ",",
                        format(measureAgg, measureColumn),
                        queries,
                        groupBy);
            }
        }


        String query = null;
        Optional<AggregationType> intermediateAggregation = getIntermediateAggregation(measureType.aggregation);

        if (intermediateAggregation.isPresent()) {
            if (grouping != null && segment != null) {
                if (grouping.type == COLUMN && segment.type == COLUMN) {
                    query = format(" SELECT " +
                                    " CASE WHEN group_rank > 15 THEN 'Others' ELSE cast(%s_group as varchar) END,\n" +
                                    " CASE WHEN segment_rank > 20 THEN 'Others' ELSE cast(%s_segment as varchar) END,\n" +
                                    " %s FROM (\n" +
                                    "   SELECT *,\n" +
                                    "          row_number() OVER (ORDER BY %s DESC) AS group_rank,\n" +
                                    "          row_number() OVER (PARTITION BY %s ORDER BY value DESC) AS segment_rank\n" +
                                    "   FROM (%s) as data GROUP BY 1, 2, 3) as data GROUP BY 1, 2 ORDER BY 3 DESC",
                            getColumnReference(grouping),
                            getColumnReference(segment),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            format(getColumnReference(grouping), "value") + "_group",
                            computeQuery);
                }
            } else {
                String columnValue = null;
                boolean reference;

                if (segment != null) {
                    columnValue = getColumnValue(segment);
                    reference = segment.type == REFERENCE;
                } else if (grouping != null) {
                    columnValue = getColumnValue(grouping);
                    reference = grouping.type == REFERENCE;
                } else {
                    reference = false;
                }

                if (columnValue != null && !reference) {
                    query = format(" SELECT " +
                                    " CASE WHEN group_rank > 50 THEN 'Others' ELSE CAST(%s as varchar) END, %s FROM (\n" +
                                    "   SELECT *, row_number() OVER (ORDER BY %s DESC) AS group_rank\n" +
                                    "   FROM (%s) as data GROUP BY 1, 2) as data GROUP BY 1 ORDER BY 2 DESC",
                            columnValue + "_group",
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            computeQuery);
                } else {
                    query = computeQuery + " ORDER BY 1 DESC LIMIT 100";
                }
            }
        }

        if (query == null) {
            query = format("select %s %s %s value from (%s) data ORDER BY %s DESC LIMIT 100",
                    grouping == null ? "" : format(grouping.type == COLUMN ? "cast(%s_group as varchar)" : "%s_group", getColumnReference(grouping)),
                    segment == null ? "" : ((grouping == null ? "" : ",") + format(segment.type == COLUMN ? "cast(%s_segment as varchar)" : "%s_segment", getColumnReference(segment))),
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

    private boolean testFilterExpressionForPerComputedTable(Expression filterExp, OLAPTable options) {
        final boolean[] columnExists = {true};

        ExpressionUtil.accept(filterExp, new DefaultExpressionTraversalVisitor<Void, Void>() {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context) {
                if (node.getName().getParts().size() != 1) {
                    columnExists[0] = false;
                }

                if (!options.dimensions.contains(node.getName().getParts().get(0))) {
                    columnExists[0] = false;
                }

                return null;
            }
        }, null);

        return columnExists[0];
    }

    private String getFinalForAggregationFunction(Measure aggregation) {
        switch (aggregation.aggregation) {
            case AVERAGE:
                return "sum(%1$s) / count(%1$s)";
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
    public CompletableFuture<QueryResult> getEventStatistics(String project, Optional<Set<String>> collections, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        checkProject(project);
        Set<String> collectionNames = metastore.getCollectionNames(project).stream()
                .filter(c -> !c.startsWith("_"))
                .collect(Collectors.toSet());

        if (collections.isPresent()) {
            for (String name : collections.get()) {
                if (!collectionNames.contains(name)) {
                    throw new RakamException(BAD_REQUEST);
                }
            }
            collectionNames = collections.get();
        }
        if (collectionNames.isEmpty()) {
            return CompletableFuture.completedFuture(QueryResult.empty());
        }

        if (dimension.isPresent()) {
            checkReference(dimension.get(), startDate, endDate, collectionNames.size());
        }

        String query;
        if (dimension.isPresent()) {
            Optional<TimestampTransformation> aggregationMethod = TimestampTransformation.fromPrettyName(dimension.get());
            if (!aggregationMethod.isPresent()) {
                throw new RakamException(BAD_REQUEST);
            }

            query = format("select collection, %s as %s, sum(total) from (", aggregationMethod.get() == HOUR ? "_time" : format(timestampMapping.get(aggregationMethod.get()), "_time"), aggregationMethod.get()) +
                    collectionNames.stream()
                            .map(collection ->
                                    format("select cast('%s' as varchar) as collection, _time, coalesce(total, 0) as total from continuous.\"%s\" ",
                                            collection,
                                            "_total_" + collection))
                            .collect(Collectors.joining(" union all ")) +
                    format(") as data where \"_time\" between date '%s' and date '%s' + interval '1' day group by 1, 2 order by 2 desc", startDate.format(ISO_DATE), endDate.format(ISO_DATE));
        } else {
            query = collectionNames.stream()
                    .map(collection ->
                            format("select cast('%s' as varchar) as collection, coalesce(sum(total), 0) as total from continuous.\"%s\" where _time between date '%s' and date '%s' + interval '1' day",
                                    collection,
                                    "_total_" + collection,
                                    startDate.format(ISO_DATE), endDate.format(ISO_DATE)))
                    .collect(Collectors.joining(" union all ")) + " order by 2 desc";
        }

        return executor.executeQuery(project, query, 20000).getResult();
    }

    @Override
    public List<String> getExtraDimensions(String project) {
        return timestampMapping.keySet().stream()
                .map(TimestampTransformation::getPrettyName)
                .collect(Collectors.toList());
    }

    public abstract String convertSqlFunction(AggregationType aggType);
}
