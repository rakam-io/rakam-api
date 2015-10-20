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

import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.EventExplorer;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.realtime.AggregationType;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.time.LocalDate;
import java.time.ZoneId;
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
import static java.util.Locale.ENGLISH;
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
import static org.rakam.analysis.EventExplorer.TimestampTransformation.fromString;
import static org.rakam.realtime.AggregationType.COUNT;
import static org.rakam.report.PrestoContinuousQueryService.PRESTO_STREAMING_CATALOG_NAME;
import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoEventExplorer implements EventExplorer {

    private final static String TIME_INTERVAL_ERROR_MESSAGE = "Date interval is too big.";

    private final PrestoQueryExecutor executor;
    private final Metastore metastore;
    private final Map<TimestampTransformation, String> timestampMapping = ImmutableMap.
            <TimestampTransformation, String>builder()
            .put(HOUR_OF_DAY, "hour(%s) as time")
            .put(DAY_OF_MONTH, "day(%s) as time")
            .put(WEEK_OF_YEAR, "week(%s) as time")
            .put(MONTH_OF_YEAR, "month(%s) as time")
            .put(QUARTER_OF_YEAR, "quarter(%s) as time")
//                    .put(DAY_PART, "CASE WHEN hour(from_unixtime(time)) > 12 THEN 'afternoon' ELSE 'night' END")
            .put(DAY_OF_WEEK, "day_of_week(%s) as time")
            .put(HOUR, "date_trunc('hour', %s) as time")
            .put(DAY, "cast(%s as date) as time")
            .put(MONTH, "date_trunc('month', %s) as time")
            .put(YEAR, "date_trunc('year', %s) as time")
            .build();

    @Inject
    public PrestoEventExplorer(PrestoQueryExecutor executor, Metastore metastore) {
        this.executor = executor;
        this.metastore = metastore;
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

    private String getColumnValue(Reference ref) {
        switch (ref.type) {
            case COLUMN:
                return ref.value;
            case REFERENCE:
                return format(timestampMapping.get(fromString(ref.value.replace(" ", "_"))), "from_unixtime(time)");
            default:
                throw new IllegalArgumentException("Unknown reference type: " + ref.value);
        }
    }

    private String getColumnReference(Reference ref) {
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

    public CompletableFuture<QueryResult> analyze(String project, List<String> collections, Measure measureType, Reference grouping, Reference segment, String filterExpression, LocalDate startDate, LocalDate endDate) {
        if (grouping !=null && grouping.type == ReferenceType.REFERENCE) {
            checkReference(grouping.value, startDate, endDate);
        }
        if (segment !=null && segment.type == ReferenceType.REFERENCE) {
            checkReference(segment.value, startDate, endDate);
        }
        String select = Stream.of(grouping, segment)
                .filter(col -> col != null)
                .map(this::getColumnValue)
                .collect(Collectors.joining(", "));

        String groupBy = Stream.of(
                grouping == null ? null : "1", segment == null ? null : "2")
                .filter(col -> col != null)
                .collect(Collectors.joining(", "));

        ZoneId utc = ZoneId.of("UTC");
        String where = Stream.of(
                startDate == null ? null : format(" time >= %s ", startDate.atStartOfDay().atZone(utc).toEpochSecond()),
                endDate == null ? null : format(" time <= %s ", endDate.atStartOfDay().atZone(utc).toEpochSecond()),
                filterExpression).filter(condition -> condition != null)
                .collect(Collectors.joining(" and "));

        String measureAgg = convertPrestoFunction(measureType != null &&
                measureType.aggregation != null ? measureType.aggregation : COUNT);
        String measureColumn = measureType != null &&
                measureType.column != null ? measureType.column : "*";

        String computeQuery;
        if (collections.size() == 1) {
            computeQuery = format("select %s %s(%s) as %s from %s where %s group by %s",
                    select.isEmpty() ? select : select + ",",
                    measureAgg,
                    measureColumn,
                    measureAgg,
                    collections.get(0),
                    where,
                    groupBy);
        } else {
            String selectPart = Stream.of(grouping, segment)
                    .filter(col -> col != null)
                    .map(this::getColumnReference)
                    .collect(Collectors.joining(", "));

            String queries = "(" + collections.stream()
                    .map(collection -> format("select %s, %s from %s where %s", select, measureColumn, collection, where))
                    .collect(Collectors.joining(" union ")) + ")";
            computeQuery = format("select %s %s(%s) as %s from %s group by %s",
                    select.isEmpty() ? "" : selectPart + ",",
                    measureAgg,
                    measureColumn,
                    measureAgg,
                    queries,
                    groupBy);
        }

        String query = null;
        Optional<AggregationType> intermediateAggregation = getIntermediateAggregation(measureType.aggregation);

        if (intermediateAggregation.isPresent()) {
            if (grouping != null && segment != null) {
                if(isGroupingSupported(project, collections, grouping) &&
                        isGroupingSupported(project, collections, segment)) {
                    query = format(" SELECT " +
                                    " CASE WHEN group_rank > 15 THEN 'Others' ELSE %s END,\n" +
                                    " CASE WHEN segment_rank > 20 THEN 'Others' ELSE %s END,\n" +
                                    " %s(%s) FROM (\n" +
                                    "   SELECT *,\n" +
                                    "          row_number() OVER (ORDER BY %s(%s) DESC) AS group_rank,\n" +
                                    "          row_number() OVER (PARTITION BY %s ORDER BY %s DESC) AS segment_rank\n" +
                                    "   FROM (%s) GROUP BY 1, 2, 3) GROUP BY 1, 2 ORDER BY 3 DESC",
                            getColumnReference(grouping),
                            getColumnReference(segment),
                            convertPrestoFunction(intermediateAggregation.get()),
                            measureAgg,
                            convertPrestoFunction(intermediateAggregation.get()),
                            measureAgg,
                            getColumnReference(grouping),
                            measureAgg,
                            computeQuery);
                } else {
                    String groupingColumn;
                    String segmentColumn;

                    if (isGroupingSupported(project, collections, segment)) {
                        segmentColumn = format("CASE WHEN group_rank > 15 THEN 'Others' ELSE %s END", getColumnValue(segment));
                    } else {
                        segmentColumn = getColumnValue(segment);
                    }

                    if (isGroupingSupported(project, collections, grouping)) {
                        groupingColumn = format("CASE WHEN group_rank > 15 THEN 'Others' ELSE %s END", getColumnValue(grouping));
                    } else {
                        groupingColumn = getColumnReference(grouping);
                    }

                    query = format(" SELECT " +
                                    " %s, %s, %s(%s) FROM (\n" +
                                    "   SELECT *, row_number() OVER (ORDER BY %s(%s) DESC) AS group_rank\n" +
                                    "   FROM (%s) GROUP BY 1, 2, 3) GROUP BY 1, 2 ORDER BY 3 DESC",
                            groupingColumn,
                            segmentColumn,
                            convertPrestoFunction(intermediateAggregation.get()),
                            measureAgg,
                            convertPrestoFunction(intermediateAggregation.get()),
                            measureAgg,
                            computeQuery);
                }
            } else {
                String columnValue = null;

                if (segment != null && isGroupingSupported(project, collections, segment)) {
                    columnValue = getColumnValue(segment);
                } else if (grouping != null && isGroupingSupported(project, collections, grouping)) {
                    columnValue = getColumnValue(grouping);
                }

                if (columnValue != null) {
                    query = format(" SELECT " +
                                    " CASE WHEN group_rank > 50 THEN 'Others' ELSE %s END, %s(%s) FROM (\n" +
                                    "   SELECT *, row_number() OVER (ORDER BY %s(%s) DESC) AS group_rank\n" +
                                    "   FROM (%s) GROUP BY 1, 2) GROUP BY 1 ORDER BY 2 DESC",
                            columnValue,
                            convertPrestoFunction(intermediateAggregation.get()),
                            measureAgg,
                            convertPrestoFunction(intermediateAggregation.get()),
                            measureAgg,
                            computeQuery);
                } else {
                    query = computeQuery + " ORDER BY 2 DESC LIMIT 100";
                }
            }
        }

        if (query == null) {
            query = computeQuery + format(" ORDER BY %s DESC LIMIT 100", segment != null && grouping != null ? 3 : 2);
        }

        return executor.executeQuery(project, query).getResult();
    }


    private boolean isGroupingSupported(String project, List<String> collections, Reference ref) {
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

    public CompletableFuture<QueryResult> getEventStatistics(String project, Optional<String> dimension, LocalDate startDate, LocalDate endDate) {
        checkProject(project);
        Set<String> collectionNames = metastore.getCollectionNames(project);

        String query;
        if (dimension.isPresent()) {
            TimestampTransformation aggregationMethod = TimestampTransformation.fromString(dimension.get().replace(" ", "_"));
            query = format("select collection, %s, total from (", format(timestampMapping.get(aggregationMethod), "time")) +
                    collectionNames.stream()
                            .map(collection ->
                                    format("select '%s' as collection, from_unixtime(time*3600) time, total from %s.%s._total_%s ",
                                            collection,
                                            PRESTO_STREAMING_CATALOG_NAME,
                                            project,
                                            collection))
                            .collect(Collectors.joining(" union "))+
                    format(") where time between date '%s' and date '%s' + interval '1' day", startDate.format(ISO_DATE), endDate.format(ISO_DATE));
        } else {
            query = collectionNames.stream()
                            .map(collection ->
                                    format("select '%s' as collection, sum(total) from %s.%s._total_%s where time between to_unixtime(date '%s')/3600 and to_unixtime(date '%s' + interval '1' day)/3600",
                                            collection,
                                            PRESTO_STREAMING_CATALOG_NAME,
                                            project,
                                            collection,
                                            startDate.format(ISO_DATE), endDate.format(ISO_DATE)))
                            .collect(Collectors.joining(" union "));
        }

        return executor.executeRawQuery(query).getResult();
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

    private String convertPrestoFunction(AggregationType aggType) {
        switch (aggType) {
            case AVERAGE:
                return "avg";
            case MAXIMUM:
                return "max";
            case MINIMUM:
                return "min";
            case COUNT:
                return "count";
            case SUM:
                return "sum";
            case APPROXIMATE_UNIQUE:
                return "approx_distinct";
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
    }


}
