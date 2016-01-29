package org.rakam.realtime;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import io.airlift.units.Duration;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.TimestampToEpochFunction;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.RealTimeConfig;
import org.rakam.report.QueryExecutor;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonResponse;
import org.rakam.util.NotImplementedException;
import org.rakam.util.RakamException;

import javax.inject.Inject;
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

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Singleton
@Api(value = "/realtime", description = "Realtime module", tags = "realtime",
        authorizations = @Authorization(value = "read_key"))
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
    @ApiOperation(value = "Create report")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/create")
    public CompletableFuture<JsonResponse> create(@ParamBody RealTimeReport report) {
        if(report.aggregation == AggregationType.AVERAGE) {
            throw new RakamException("Average aggregation is not supported in realtime service.", BAD_REQUEST);
        }

        String sqlQuery = new StringBuilder().append("select ")
                .append(format("(cast(" + timestampToEpochFunction + "(_time) as bigint) / %d) as _time, ", slide.roundTo(TimeUnit.SECONDS)))
                .append(createFinalSelect(report.aggregation, report.measure, report.dimensions))
                .append(" FROM (" + report.collections.stream().map(col -> "(SELECT " + createSelect(report.measure, report.dimensions) + " FROM " + col + ") as data").collect(Collectors.joining(" UNION ALL ")) + ")")
                .append(report.filter == null ? "" : " where " + report.filter)
                .append(" group by 1 ")
                .append(report.dimensions != null ?
                        IntStream.range(0, report.dimensions.size()).mapToObj(i -> ", " + (i + 2)).collect(Collectors.joining("")) : "")
                .toString();

        ContinuousQuery query = new ContinuousQuery(report.project,
                report.name,
                report.table_name,
                sqlQuery,
                ImmutableList.of(),
                ImmutableMap.of("realtime", true, "aggregation", report.aggregation));
        return service.create(query).thenApply(JsonResponse::map);
    }
    
    @JsonRequest
    @ApiOperation(value = "List queries")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/list")
    public List<ContinuousQuery> list(@ApiParam(name = "project") String project) {
        return service.list(project).stream()
                .filter(c -> TRUE.equals(c.options.get("realtime")))
                .collect(Collectors.toList());
    }

    @JsonRequest
    @POST
    @ApiOperation(value = "Get report")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist."),
            @ApiResponse(code = 400, message = "Report does not exist.")})
    @Path("/get")
    public CompletableFuture<RealTimeQueryResult> get(@ApiParam(name = "project") String project,
                                                      @ApiParam(name = "table_name") String tableName,
                                                      @ApiParam(name = "filter", required = false) String filter,
                                                      @ApiParam(name = "dimensions", required = false) List<String> dimensions,
                                                      @ApiParam(name = "aggregate", required = false) Boolean aggregate,
                                                      @ApiParam(name = "date_start", required = false) Instant dateStart,
                                                      @ApiParam(name = "date_end", required = false) Instant dateEnd) {
        Expression expression;
        if (filter != null) {
            expression = sqlParser.createExpression(filter);
        } else {
            expression = null;
        }

        if(aggregate == null) {
            aggregate = false;
        }

        boolean noDimension = dimensions == null || dimensions.size() == 0;

        ContinuousQuery continuousQuery = service.get(project, tableName);
        if (continuousQuery == null) {
            CompletableFuture<RealTimeQueryResult> f = new CompletableFuture<>();
            f.completeExceptionally(new RakamException("Couldn't found rule", HttpResponseStatus.BAD_REQUEST));
            return f;
        }

        String aggregation = (String) continuousQuery.options.get("aggregation");
        if(!TRUE.equals(continuousQuery.options.get("realtime")) || aggregation == null) {
            CompletableFuture<RealTimeQueryResult> f = new CompletableFuture<>();
            f.completeExceptionally(new RakamException("Invalid rule.", HttpResponseStatus.BAD_REQUEST));
            return f;
        }

        AggregationType sourceAggregation = AggregationType.valueOf(aggregation);

        long last_update = Instant.now().toEpochMilli() - slide.toMillis();
        long previousWindow = (dateStart == null ? (last_update - window.toMillis()) : dateStart.toEpochMilli()) / (slide.toMillis());
        long currentWindow = (dateEnd == null ? last_update : dateEnd.toEpochMilli()) / slide.toMillis();

        Object timeCol = aggregate ? currentWindow : "_time";
        String sqlQuery = format("select %s, %s %s(%s) from %s where %s %s %s ORDER BY 1 ASC LIMIT 5000",
                timeCol + " * " + slide.toMillis(),
                !noDimension ? dimensions.stream().collect(Collectors.joining(", ")) + "," : "",
                combineFunction(sourceAggregation),
                "value",
                executor.formatTableReference(continuousQuery.project, QualifiedName.of("continuous", continuousQuery.tableName)),
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
                    return new RealTimeQueryResult(previousTimestamp, currentTimestamp, data.size() > 0 ? data.get(0).get(1) : 0);
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
    @ApiOperation(value = "Delete report")
    @Path("/delete")
    public CompletableFuture<JsonResponse> delete(@ApiParam(name = "project") String project,
                                                  @ApiParam(name = "table_name") String tableName) {

        // TODO: Check if it's a real-time report.
        return service.delete(project, tableName).thenApply(result -> {
            if (result) {
                return JsonResponse.success();
            } else {
                return JsonResponse.error("Couldn't delete report. Most probably it doesn't exist");
            }
        });

    }

    private String createSelect(String measure, List<String> dimensions) {
        if (measure == null) {
            measure = "1";
        }
        String collect = dimensions.stream().collect(Collectors.joining(", "));
        return "_time, " + measure + (collect.isEmpty() ? "" : "," + collect);
    }

    private String createFinalSelect(AggregationType aggType, String measure, List<String> dimensions) {

        if (measure == null) {
            if (aggType != AggregationType.COUNT)
                throw new IllegalArgumentException("either measure.expression or measure.field must be specified.");
            else {
                measure = "*";
            }
        }

        StringBuilder builder = new StringBuilder();
        if (dimensions != null && !dimensions.isEmpty())
            builder.append(" " + dimensions.stream().collect(Collectors.joining(", ")) + ", ");

        switch (aggType) {
            case MAXIMUM:
                return builder.append(format("max(%s) as value", measure)).toString();
            case MINIMUM:
                return builder.append(format("min(%s) as value", measure)).toString();
            case COUNT:
                return builder.append(format("count(%s) as value", measure)).toString();
            case SUM:
                return builder.append(format("sum(%s) as value", measure)).toString();
            case APPROXIMATE_UNIQUE:
                return builder.append(format("approx_set(%s) as value", measure)).toString();
            default:
                throw new IllegalArgumentException("aggregation type couldn't found.");
        }
    }

    private String mapFunction(AggregationType aggregationType) {
        switch (aggregationType) {
            case COUNT:
            case SUM:
                return aggregationType.value();
            case MINIMUM:
                return "min";
            case MAXIMUM:
                return "max";
            case APPROXIMATE_UNIQUE:
                return "approx_set";
            default:
                throw new NotImplementedException();
        }
    }

    private String combineFunction(AggregationType aggregationType) {
        switch (aggregationType) {
            case COUNT:
            case SUM:
                return "sum";
            case MINIMUM:
                return "min";
            case MAXIMUM:
                return "max";
            case APPROXIMATE_UNIQUE:
                return "merge";
            default:
                throw new NotImplementedException();
        }
    }
}
