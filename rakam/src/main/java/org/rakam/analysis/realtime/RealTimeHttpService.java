package org.rakam.analysis.realtime;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import io.airlift.units.Duration;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.TimestampToEpochFunction;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryExecutor;
import org.rakam.report.realtime.AggregationType;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.report.realtime.RealTimeReport;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;
import org.rakam.util.NotImplementedException;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.rakam.util.ValidationUtil.checkTableColumn;

@Singleton
@Api(value = "/realtime", nickname = "realtime", description = "Realtime module", tags = "realtime")
@Path("/realtime")
public class RealTimeHttpService extends HttpService {
    private final ContinuousQueryService service;
    private final QueryExecutor executor;
    private final SqlParser sqlParser = new SqlParser();
    private final Duration slide;
    private final Duration window;

    private final String timestampToEpochFunction;

    @Inject
    public RealTimeHttpService(ContinuousQueryService service, QueryExecutor executor, RealTimeConfig config, @TimestampToEpochFunction String timestampToEpochFunction) {
        this.service = requireNonNull(service, "service is null");
        this.executor = requireNonNull(executor, "executor is null");
        RealTimeConfig realTimeConfig = requireNonNull(config, "config is null");
        this.window = realTimeConfig.getWindowInterval();
        this.slide = realTimeConfig.getSlideInterval();
        this.timestampToEpochFunction = requireNonNull(timestampToEpochFunction, "timestampToEpochFunction is null");
    }

    /**
     * Creates real-time report using continuous queries.
     * This module adds a new attribute called 'time' to events, it's simply a unix epoch that represents the seconds the event is occurred.
     * Continuous query continuously aggregates 'time' column and
     * real-time module executes queries on continuous query table similar to 'select count from stream_count where time &gt; now() - interval 5 second'
     * <p>
     * curl 'http://localhost:9999/realtime/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Events by collection", "aggregation": "COUNT"}'
     *
     * @param report real-time report
     * @return a future that contains the operation status
     */
    @JsonRequest
    @ApiOperation(value = "Create report", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public CompletableFuture<JsonResponse> createTable(@Named("project") String project, @BodyParam RealTimeReport report) {
        String sqlQuery = new StringBuilder().append("select ")
                .append(format("(cast(" + timestampToEpochFunction + "(_time) as bigint) / %d) as _time, ", slide.roundTo(TimeUnit.SECONDS)))
                .append(createFinalSelect(report.measures, report.dimensions))
                .append(" FROM (" + report.collections.stream().map(col -> String.format("(SELECT %s FROM %s) as data",
                        Stream.of("_time", report.dimensions.stream().collect(Collectors.joining(", ")),
                                report.measures.stream().map(e -> e.column).distinct().collect(Collectors.joining(", "))).filter(e -> !e.isEmpty()).collect(Collectors.joining(", ")), col
                )).collect(Collectors.joining(" UNION ALL ")) + ")")
                .append(report.filter == null ? "" : " where " + report.filter)
                .append(" group by 1 ")
                .append(report.dimensions != null ?
                        IntStream.range(0, report.dimensions.size()).mapToObj(i -> ", " + (i + 2)).collect(Collectors.joining("")) : "")
                .toString();

        ContinuousQuery query = new ContinuousQuery(
                report.name,
                report.table_name,
                sqlQuery,
                ImmutableList.of(),
                ImmutableMap.of("realtime", true, "aggregation", report.measures));
        return service.create(project, query, false).getResult().thenApply(JsonResponse::map);
    }

    @JsonRequest
    @ApiOperation(value = "List queries", authorizations = @Authorization(value = "read_key"))

    @Path("/list")
    public List<ContinuousQuery> listTables(@javax.inject.Named("project") String project) {
        return service.list(project).stream()
                .filter(c -> TRUE.equals(c.options.get("realtime")))
                .collect(Collectors.toList());
    }

    @JsonRequest
    @POST
    @ApiOperation(value = "Get report", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Report does not exist.")})
    @Path("/get")
    public CompletableFuture<RealTimeQueryResult> queryTable(@javax.inject.Named("project") String project,
                                                      @ApiParam("table_name") String tableName,
                                                      @ApiParam(value = "filter", required = false) String filter,
                                                      @ApiParam("measure") RealTimeReport.Measure measure,
                                                      @ApiParam(value = "dimensions", required = false) List<String> dimensions,
                                                      @ApiParam(value = "aggregate", required = false) Boolean aggregate,
                                                      @ApiParam(value = "date_start", required = false) Instant dateStart,
                                                      @ApiParam(value = "date_end", required = false) Instant dateEnd) {
        Expression expression;
        if (filter != null) {
            expression = sqlParser.createExpression(filter);
        } else {
            expression = null;
        }

        if (aggregate == null) {
            aggregate = false;
        }

        boolean noDimension = dimensions == null || dimensions.isEmpty();

        long last_update = Instant.now().toEpochMilli() - slide.toMillis();
        long previousWindow = (dateStart == null ? (last_update - window.toMillis()) : dateStart.toEpochMilli()) / (slide.toMillis());
        long currentWindow = (dateEnd == null ? last_update : dateEnd.toEpochMilli()) / slide.toMillis();

        Object timeCol = aggregate ? currentWindow : "_time";
        String sqlQuery = format("select %s, %s %s from %s where %s %s %s ORDER BY 1 ASC LIMIT 5000",
                timeCol + " * cast(" + slide.toMillis()+" as bigint)",
                !noDimension ? dimensions.stream().collect(Collectors.joining(", ")) + "," : "",
                String.format(combineFunction(measure.aggregation), checkTableColumn(measure.column, "measure column is not valid") + "_" + measure.aggregation.name().toLowerCase()),
                executor.formatTableReference(project, QualifiedName.of("continuous", tableName)),
                format("_time >= %d", previousWindow) +
                        (dateEnd == null ? "" :
                                format("AND _time <", format("_time >= %d AND _time <= %d", previousWindow, currentWindow))),
                !noDimension || !aggregate ? format("GROUP BY %s %s %s", !aggregate ? timeCol : "", !aggregate && !noDimension ? "," : "", dimensions.stream().collect(Collectors.joining(", "))) : "",
                (expression == null) ? "" : formatExpression(expression, reference -> executor.formatTableReference(project, reference)));

        final boolean finalAggregate = aggregate;
        return executor.executeRawQuery(sqlQuery).getResult().thenApply(result -> {
            if (result.isFailed()) {
                // TODO: be sure that this exception is catched
                throw new RakamException(result.getError().message, INTERNAL_SERVER_ERROR);
            }

            long previousTimestamp = previousWindow * slide.toMillis() / 1000;
            long currentTimestamp = currentWindow * slide.toMillis() / 1000;

            List<List<Object>> data = result.getResult();

            if (!finalAggregate) {
                if (noDimension) {
                    List<List<Object>> newData = Lists.newLinkedList();
                    Map<Long, List<Object>> collect = data.stream().collect(Collectors.toMap(a -> (Long) a.get(0), a -> a));
                    for (long current = previousWindow * slide.toMillis(); current < currentWindow * slide.toMillis(); current += slide.toMillis()) {

                        List<Object> objects = collect.get(current);

                        if (objects != null) {
                            ArrayList<Object> list = new ArrayList<>(2);
                            list.add(current);

                            list.add(objects.get(dimensions.size() + 1));
                            newData.add(list);
                            continue;
                        }

                        ArrayList<Object> list = new ArrayList<>(2);
                        list.add(current);

                        list.add(0);
                        newData.add(list);
                    }
                    return new RealTimeQueryResult(previousTimestamp, currentTimestamp, newData);
                } else {
                    return new RealTimeQueryResult(previousTimestamp, currentTimestamp, data);
                }
            } else {
                if (noDimension) {
                    return new RealTimeQueryResult(previousTimestamp, currentTimestamp, !data.isEmpty() ? data.get(0).get(1) : 0);
                } else {
                    return new RealTimeQueryResult(previousTimestamp, currentTimestamp, data);
                }
            }
        });
    }

    public static class RealTimeQueryResult {
        public final long start;
        public final long end;
        public final Object result;

        public RealTimeQueryResult(long start, long end, Object result) {
            this.start = start;
            this.end = end;
            this.result = result;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Delete report", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    public CompletableFuture<JsonResponse> deleteTable(@javax.inject.Named("project") String project,
                                                  @ApiParam("table_name") String tableName) {

        // TODO: Check if it's a real-time report.
        return service.delete(project, tableName).thenApply(result -> {
            if (result) {
                return JsonResponse.success();
            } else {
                return JsonResponse.error("Couldn't delete report. Most probably it doesn't exist");
            }
        });

    }

    private String createFinalSelect(List<RealTimeReport.Measure> measures, List<String> dimensions) {
        StringBuilder builder = new StringBuilder();
        if (dimensions != null && !dimensions.isEmpty())
            builder.append(" " + dimensions.stream().collect(Collectors.joining(", ")) + ", ");

        for (int i = 0; i < measures.size(); i++) {
            if (measures.get(i).aggregation == AggregationType.AVERAGE) {
                throw new RakamException("Average aggregation is not supported in realtime service.", BAD_REQUEST);
            }

            String format;
            switch (measures.get(i).aggregation) {
                case MAXIMUM:
                    format = "max(%s)";
                    break;
                case MINIMUM:
                    format = "min(%s)";
                    break;
                case COUNT:
                    format = "count(%s)";
                    break;
                case SUM:
                    format = "sum(%s)";
                    break;
                case APPROXIMATE_UNIQUE:
                    format = "approx_set(%s)";
                    break;
                default:
                    throw new IllegalArgumentException("aggregation type couldn't found.");
            }

            builder.append(String.format(format + " as %s_%s ", measures.get(i).column,
                    measures.get(i).column, measures.get(i).aggregation.name().toLowerCase()));
            if (i < measures.size() - 1) {
                builder.append(", ");
            }
        }

        return builder.toString();
    }

    private String combineFunction(AggregationType aggregationType) {
        switch (aggregationType) {
            case COUNT:
            case SUM:
                return "sum(%s)";
            case MINIMUM:
                return "min(%s)";
            case MAXIMUM:
                return "max(%s)";
            case APPROXIMATE_UNIQUE:
                return "cardinality(merge(%s))";
            default:
                throw new NotImplementedException();
        }
    }
}
