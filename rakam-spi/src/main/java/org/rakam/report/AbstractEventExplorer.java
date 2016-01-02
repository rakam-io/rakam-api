package org.rakam.report;

import com.facebook.presto.sql.tree.QualifiedName;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.EventExplorer;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.realtime.AggregationType;
import org.rakam.util.RakamException;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Character.toUpperCase;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Locale.ENGLISH;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.fromString;
import static org.rakam.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkProject;

public abstract class AbstractEventExplorer implements EventExplorer {
    private final static String TIME_INTERVAL_ERROR_MESSAGE = "Date interval is too big.";

    private final QueryExecutor executor;

    private final Metastore metastore;
    private final Map<EventExplorer.TimestampTransformation, String> timestampMapping;
    private final QueryExecutorService service;

    public AbstractEventExplorer(QueryExecutor executor, QueryExecutorService service, Metastore metastore, Map<TimestampTransformation, String> timestampMapping) {
        this.executor = executor;
        this.service = service;
        this.metastore = metastore;
        this.timestampMapping = timestampMapping;
    }

    private void checkReference(String refValue, LocalDate startDate, LocalDate endDate) {
        switch (fromString(refValue.replace(" ", "_"))) {
            case HOUR_OF_DAY:
            case DAY_OF_MONTH:
            case WEEK_OF_YEAR:
            case MONTH_OF_YEAR:
            case QUARTER_OF_YEAR:
            case DAY_OF_WEEK:
                return;
            case HOUR:
                if (startDate.atStartOfDay().until(endDate.atStartOfDay(), ChronoUnit.HOURS) > 3000)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, HttpResponseStatus.BAD_REQUEST);
                break;
            case DAY:
                if (startDate.until(endDate, ChronoUnit.DAYS) > 100)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, HttpResponseStatus.BAD_REQUEST);
                break;
            case MONTH:
                if (startDate.until(endDate, ChronoUnit.MONTHS) > 100)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, HttpResponseStatus.BAD_REQUEST);
                break;
            case YEAR:
                if (startDate.until(endDate, ChronoUnit.YEARS) > 100)
                    throw new RakamException(TIME_INTERVAL_ERROR_MESSAGE, HttpResponseStatus.BAD_REQUEST);
                break;
        }

    }

    private String getColumnValue(EventExplorer.Reference ref) {
        switch (ref.type) {
            case COLUMN:
                return ref.value;
            case REFERENCE:
                return format(timestampMapping.get(fromString(ref.value.replace(" ", "_"))), "_time");
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    private String getColumnReference(EventExplorer.Reference ref) {
        switch (ref.type) {
            case COLUMN:
                return ref.value;
            case REFERENCE:
                return "time";
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

    public CompletableFuture<QueryResult> analyze(String project, List<String> collections, EventExplorer.Measure measureType, EventExplorer.Reference grouping, EventExplorer.Reference segment, String filterExpression, LocalDate startDate, LocalDate endDate) {
        if (grouping != null && grouping.type == EventExplorer.ReferenceType.REFERENCE) {
            checkReference(grouping.value, startDate, endDate);
        }
        if (segment != null && segment.type == EventExplorer.ReferenceType.REFERENCE) {
            checkReference(segment.value, startDate, endDate);
        }
        StringBuilder selectBuilder = new StringBuilder();
        if(grouping != null) {
            selectBuilder.append(getColumnValue(grouping)+" as "+getColumnReference(grouping)+"_group");
            if(segment != null) {
                selectBuilder.append(", ");
            }
        }
        if(segment != null) {
            selectBuilder.append(getColumnValue(segment)+" as "+getColumnReference(segment)+"_segment");
        }
        String select = selectBuilder.toString();

        String groupBy;
        if (segment != null && grouping != null) {
            groupBy = "1, 2";
        } else if (segment != null || grouping != null) {
            groupBy = "1";
        } else {
            groupBy = "";
        }

        String where = Stream.of(
                format(" _time between date '%s' and date '%s' + interval '1' day", startDate.format(ISO_LOCAL_DATE), endDate.format(ISO_LOCAL_DATE)),
                filterExpression)
                .filter(condition -> condition != null && !condition.isEmpty())
                .collect(Collectors.joining(" and "));

        String measureAgg = convertSqlFunction(measureType != null &&
                measureType.aggregation != null ? measureType.aggregation : COUNT);
        String measureColumn = measureType != null &&
                measureType.column != null ? measureType.column : "*";

        String computeQuery;
        if (collections.size() == 1) {
            computeQuery = format("select %s %s as value from %s where %s group by %s",
                    select.isEmpty() ? select : select + ",",
                    format(measureAgg, measureColumn),
                    executor.formatTableReference(project, QualifiedName.of(collections.get(0))),
                    where,
                    groupBy);
        } else {
            String selectPart = (grouping == null ? "": getColumnReference(grouping)+"_group") +
            (segment == null ? "":
                    (grouping == null ? "" : ", ")+getColumnReference(segment)+"_segment");

            String queries = "(" + collections.stream()
                    .map(collection -> format("select %s %s from %s where %s",
                            select.isEmpty() ? select : select + ",",
                            measureColumn,
                            executor.formatTableReference(project, QualifiedName.of(collection)), where))
                    .collect(Collectors.joining(" union ")) + ")";
            computeQuery = format("select %s %s as value from (%s) as data group by %s",
                    select.isEmpty() ? "" : selectPart + ",",
                    format(measureAgg, measureColumn),
                    queries,
                    groupBy);
        }

        String query = null;
        Optional<AggregationType> intermediateAggregation = getIntermediateAggregation(measureType.aggregation);

        if (intermediateAggregation.isPresent()) {
            if (grouping != null && segment != null) {
                boolean groupingSupported = isGroupingSupported(project, collections, grouping);
                boolean segmentSupported = isGroupingSupported(project, collections, segment);

                query = format(" SELECT " +
                                " CASE WHEN group_rank > 15 THEN %s ELSE %s_group END,\n" +
                                " CASE WHEN segment_rank > 20 THEN %s ELSE %s_segment END,\n" +
                                " %s FROM (\n" +
                                "   SELECT *,\n" +
                                "          row_number() OVER (ORDER BY %s DESC) AS group_rank,\n" +
                                "          row_number() OVER (PARTITION BY %s ORDER BY value DESC) AS segment_rank\n" +
                                "   FROM (%s) as data GROUP BY 1, 2, 3) as data GROUP BY 1, 2 ORDER BY 3 DESC",
                        groupingSupported ? "'Others'" : "null",
                        getColumnReference(grouping),
                        segmentSupported ? "'Others'" : "null",
                        getColumnReference(segment),
                        format(convertSqlFunction(intermediateAggregation.get()), "value"),
                        format(convertSqlFunction(intermediateAggregation.get()), "value"),
                        format(getColumnReference(grouping), "value")+"_group",
                        computeQuery);
            } else {
                String columnValue = null;
                boolean group, reference;

                if (segment != null) {
                    columnValue = getColumnValue(segment);
                    group = isGroupingSupported(project, collections, segment);
                    reference = segment.type == ReferenceType.REFERENCE;
                } else if (grouping != null) {
                    columnValue = getColumnValue(grouping);
                    group = isGroupingSupported(project, collections, grouping);
                    reference = grouping.type == ReferenceType.REFERENCE;
                } else {
                    group = reference = false;
                }

                if (columnValue != null && !reference) {
                    query = format(" SELECT " +
                                    " CASE WHEN group_rank > 50 THEN %s ELSE %s END, %s FROM (\n" +
                                    "   SELECT *, row_number() OVER (ORDER BY %s DESC) AS group_rank\n" +
                                    "   FROM (%s) as data GROUP BY 1, 2) as data GROUP BY 1 ORDER BY 2 DESC",
                            group ? "'Others'" : "null",
                            columnValue+"_group",
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            format(convertSqlFunction(intermediateAggregation.get()), "value"),
                            computeQuery);
                } else {
                    query = computeQuery + " ORDER BY 2 DESC LIMIT 100";
                }
            }
        }

        if (query == null) {
            query = computeQuery + format(" ORDER BY %s DESC LIMIT 100", segment != null && grouping != null ? 3 : 2);
        }

        return executor.executeRawQuery(query).getResult();
    }


    private boolean isGroupingSupported(String project, List<String> collections, EventExplorer.Reference ref) {
        List<SchemaField> fields = null;

        switch (ref.type) {
            case COLUMN:
                if (fields == null) {
                    fields = metastore.getCollection(project, collections.get(0));
                }
                return fields.stream()
                        .filter(col -> col.getName().equals(ref.value))
                        .findAny().get().getType() == FieldType.STRING;
            case REFERENCE:
                return false;
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    public CompletableFuture<QueryResult> getEventStatistics(String project, Optional<Set<String>> collections, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        checkProject(project);
        Set<String> collectionNames = metastore.getCollectionNames(project);

        if (collections.isPresent()) {
            for (String name : collections.get()) {
                if (!collectionNames.contains(name)) {
                    throw new RakamException(HttpResponseStatus.BAD_REQUEST);
                }
            }
            collectionNames = collections.get();
        }
        if (collectionNames.isEmpty()) {
            return CompletableFuture.completedFuture(QueryResult.empty());
        }

        String query;
        if (dimension.isPresent()) {
            EventExplorer.TimestampTransformation aggregationMethod = EventExplorer.TimestampTransformation.fromString(dimension.get().replace(" ", "_"));
            query = format("select collection, %s, total from (", format(timestampMapping.get(aggregationMethod), "time")) +
                    collectionNames.stream()
                            .map(collection ->
                                    format("select '%s' as collection, time, total from continuous.\"%s\" ",
                                            collection,
                                            "_total_" + collection))
                            .collect(Collectors.joining(" union ")) +
                    format(") as data where \"time\" between date '%s' and date '%s' + interval '1' day", startDate.format(ISO_DATE), endDate.format(ISO_DATE));
        } else {
            query = collectionNames.stream()
                    .map(collection ->
                            format("select '%s' as collection, sum(total) from continuous.\"%s\" where time between date '%s' and date '%s' + interval '1' day",
                                    collection,
                                    "_total_" + collection,
                                    startDate.format(ISO_DATE), endDate.format(ISO_DATE)))
                    .collect(Collectors.joining(" union "));
        }

        return service.executeQuery(project, query, 5000).getResult();
    }

    @Override
    public List<String> getExtraDimensions(String project) {
        return timestampMapping.keySet().stream()
                .map(key -> toUpperCase(key.name().charAt(0)) + key.name().replace("_", " ").substring(1).toLowerCase(ENGLISH))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getEventDimensions(String project) {
        return timestampMapping.keySet().stream()
                .map(key -> toUpperCase(key.name().charAt(0)) + key.name().replace("_", " ").substring(1).toLowerCase(ENGLISH))
                .collect(Collectors.toList());
    }

    private String convertSqlFunction(AggregationType aggType) {
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
                return "approx_distinct(%s)";
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }


}
