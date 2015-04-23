package org.rakam.realtime;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryExecutor;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.json.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.convert;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 14:30.
 */
@Singleton
@Path("/realtime")
public class RealTimeHttpService extends HttpService {
    private final ContinuousQueryService service;
    private final QueryExecutor executor;
    SqlParser sqlParser = new SqlParser();
    private final Duration slideInterval = Duration.ofSeconds(5);
    private final Duration window = Duration.ofSeconds(45);

    private static final Pattern NONLATIN = Pattern.compile("[^\\w-]");
    private static final Pattern WHITESPACE = Pattern.compile("[\\s]");

    @Inject
    public RealTimeHttpService(ContinuousQueryService service, QueryExecutor executor) {
        this.service = checkNotNull(service, "service is null");
        this.executor = checkNotNull(executor, "executor is null");
    }

    /**
     * @api {post} /realtime/create Create realtime report
     * @apiVersion 0.1.0
     * @apiName CreateRealTimeReport
     * @apiGroup realtime
     * @apiDescription Creates realtime report using continuous queries.
     * This module adds a new attribute called 'time' to events, it's simply a unix epoch that represents the seconds the event is occurred.
     * Continuous query continuously aggregates 'time' column and
     * realtime module executes queries on continuous query table similar to 'select count from stream_count where time > now() - interval 5 second'
     * @apiError Project does not exist.
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"success": true}
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   The name of the report.
     * @apiParam {String=COUNT,SUM,MINIMUM,MAXIMUM,APPROXIMATE_UNIQUE,VARIANCE,POPULATION_VARIANCE,STANDARD_DEVIATION,AVERAGE} aggregation    The query that specifies collections that will be fetched
     * @apiParam {String} [measure]   The columns that will be used by aggregation function. If aggregation function is not COUNT, then this field is not optional.
     * @apiParam {String} [dimension]   The dimension of the realtime report. It groups the result-set based on the dimension field value.
     * @apiParam {String[]} [collections]  The collections that is the source of the report. Events that belong to these collections will be processed by the realtime report.
     * @apiParam {String} [filter] Optional SQL predicate expression that will filter the events. Referenced columns must be available all specified collections of the reports.
     * @apiParamExample {json} Request-Example:
     * {"project": "projectId", "name": "Events by collection", "aggregation": "COUNT"}
     * @apiSuccess (200) {Boolean} success Returns the success status.
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/realtime/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Events by collection", "aggregation": "COUNT"}'
     */
    @JsonRequest
    @POST
    @Path("/create")
    public CompletableFuture<JsonResponse> create(RealTimeReport query) {
        String tableName = toSlug(query.name);

        String sqlQuery = new StringBuilder().append("select ")
                .append(format("(time / %d) as time, ", slideInterval.getSeconds()))
                .append(createSelect(query.aggregation, query.measure, query.dimension))
                .append(" from stream")
                .append(query.filter == null ? "" : "where " + query.filter)
                .append(query.dimension != null ? " group by 1, 2" : " group by 2").toString();

        ContinuousQuery report = new ContinuousQuery(query.project,
                query.name,
                tableName,
                sqlQuery,
                query.collections,
                ImmutableMap.of("type", "realtime", "report", query));
        return service.create(report).thenApply(result ->
                new JsonResponse() {
                    public final boolean success = result.isFailed();
                    public final String message = result.isFailed() ? result.getError().message : null;
                });
    }

    /**
     * @api {post} /realtime/get Create realtime report
     * @apiVersion 0.1.0
     * @apiName GetRealTimeReport
     * @apiGroup realtime
     * @apiDescription Executes the query and returns data.
     * @apiError Project does not exist.
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"success": true}
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   The name of the report.
     * @apiParam [String] filter Predicate
     * @apiParam [Number] date an unix epoch value the query will be performed on.
     * @apiParamExample {json} Request-Example:
     * {"project": "projectId", "name": "Events by collection", "aggregation": "COUNT"}
     * @apiSuccess (200) {Boolean} success Returns the success status.
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/realtime/get' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Events by collection", "aggregation": "COUNT"}'
     */
    @JsonRequest
    @POST
    @Path("/get")
    public CompletableFuture<Object> get(RealTimeQuery query) {
        Expression expression;
        if (query.filter != null) {
            expression = sqlParser.createExpression(query.filter);
        } else {
            expression = null;
        }

        ContinuousQuery continuousQuery = service.get(query.project, query.name);
        if (continuousQuery == null) {
            CompletableFuture<Object> f = new CompletableFuture<>();
            f.completeExceptionally(new RakamException("Couldn't found rule", 400));
            return f;
        }

        long now = Instant.now().getEpochSecond();

        long previousWindow = (query.dateStart == null ? (now - window.getSeconds()) : query.dateStart.getEpochSecond()) / 5;
        long currentWindow = (query.dateEnd == null ? now : query.dateEnd.getEpochSecond()) / 5;

        RealTimeReport report = JsonHelper.convert(continuousQuery.options.get("report"), RealTimeReport.class);

        Object timeCol = query.aggregate ? currentWindow : "time";
        String sqlQuery = format("select %s, %s %s(value) from %s where %s %s %s ORDER BY 1 ASC",
                timeCol,
                report.dimension!=null ? report.dimension+"," : "",
                query.aggregate ? report.aggregation : "",
                "continuous." + continuousQuery.tableName,
                format("time between %d and %d", previousWindow, currentWindow),
                report.dimension!=null && query.aggregate ? "GROUP BY "+report.dimension : "",
                expression == null ? "" : ExpressionFormatter.formatExpression(expression));

        return executor.executeQuery(continuousQuery.project, sqlQuery).getResult().thenApply(result -> {
            if (!result.isFailed()) {

                String previousISO = ISO_INSTANT.format(Instant.ofEpochSecond(previousWindow*5));
                String currentISO = ISO_INSTANT.format(Instant.ofEpochSecond(currentWindow*5));

                List<List<Object>> data = result.getResult();
                if(!query.aggregate) {
                    if(report.dimension == null) {
                        List<List<Object>> newData = Lists.newLinkedList();
                        int currentDataIdx = 0;
                        for (long current = previousWindow; current < currentWindow; current++) {
                            String formattedTime = ISO_INSTANT.format(Instant.ofEpochSecond(current*5));
                            if (data.size() > currentDataIdx) {
                                List<Object> objects = data.get(currentDataIdx++);
                                Long time = ((Number) objects.get(0)).longValue();
                                if (time == current) {
                                    newData.add(ImmutableList.of(formattedTime, objects.get(1)));
                                    continue;
                                }
                            }
                            newData.add(ImmutableList.of(formattedTime, 0));
                        }
                        return new RealTimeQueryResult(previousISO, currentISO, newData);
                    } else {

                        Map<String, List<Object>> newData = data.stream()
                                .collect(Collectors.groupingBy(g ->
                                        ISO_INSTANT.format(Instant.ofEpochSecond(((Number) g.get(0)).longValue()))
                                        , TreeMap::new, Collectors.mapping(l -> ImmutableList.of(l.get(1), l.get(2)), Collectors.toList())));
                        return new RealTimeQueryResult(previousISO, currentISO, newData);
                    }
                } else {
                    if(report.dimension == null) {
                        return new RealTimeQueryResult(previousISO, currentISO, data.size() > 0 ? data.get(0).get(1) : 0);
                    } else {
                        List<ImmutableList<Object>> newData = data.stream()
                                .map(m -> ImmutableList.of(m.get(1), m.get(2)))
                                .collect(Collectors.toList());
                        return new RealTimeQueryResult(previousISO, currentISO, newData);
                    }
                }
            }
            return result;
        });
    }

    public static class RealTimeQueryResult {
        public final String start;
        public final String end;
        public final Object result;

        public RealTimeQueryResult(String start, String end, Object result) {
            this.start = start;
            this.end = end;
            this.result = result;
        }
    }

    /**
     * @api {post} /realtime/list List realtime reports
     * @apiVersion 0.1.0
     * @apiName ListRealTimeReports
     * @apiGroup realtime
     * @apiDescription List the realtime reports created for the project.
     * @apiError Project does not exist.
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"reports": [{"project": "projectId", "name": "Events by collection", "aggregation": "COUNT"}]}
     * @apiParam {String} project   Project tracker code
     * @apiParamExample {json} Request-Example:
     * {"project": "projectId"}
     * @apiSuccess (200) {Object[]} reports Returns the list of realtime reports.
     * @apiParam {String} reports.project   Project tracker code
     * @apiParam {String} reports.name   The name of the report.
     * @apiParam {String=COUNT,SUM,MINIMUM,MAXIMUM,APPROXIMATE_UNIQUE,VARIANCE,POPULATION_VARIANCE,STANDARD_DEVIATION,AVERAGE} reports.aggregation    The query that specifies collections that will be fetched
     * @apiParam {String} [reports.measure]   The columns that will be used by aggregation function. If aggregation function is not COUNT, then this field is not optional.
     * @apiParam {String} [reports.dimension]   The dimension of the realtime report.
     * @apiParam {String[]} [reports.collections]  The collections that is the source of the report. Events that belong to these collections will be processed by the realtime report.
     * @apiParam {String} [reports.filter] Optional SQL predicate expression that will filter the events.
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/realtime/list' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"project": "projectId"}'
     */
    @JsonRequest
    @POST
    @Path("/list")
    public Object list(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }
        return service.list(project.asText()).stream()
                .filter(report -> report.options != null && Objects.equals(report.options.get("type"), "realtime"))
                .map(report -> convert(report.options.get("report"), RealTimeReport.class))
                .collect(Collectors.toList());
    }


    /**
     * @api {post} /realtime/delete Delete realtime report
     * @apiVersion 0.1.0
     * @apiName DeleteRealTimeReport
     * @apiGroup realtime
     * @apiDescription Deletes specified realtime report.
     * @apiError Project does not exist.
     * @apiError Report does not exist.
     * @apiSuccessExample {json} Success-Response:
     * HTTP/1.1 200 OK
     * {"success": true}
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   The name of the report that is specified when the is created.
     * @apiParamExample {json} Request-Example:
     * {"project": "projectId", "name": "Events by collection"}
     * @apiSuccess (200) {Boolean} success Returns the success status.
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/realtime/delete' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Events by collection"}'
     */
    @JsonRequest
    @POST
    @Path("/delete")
    public Object delete(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null || !project.isTextual()) {
            return errorMessage("project parameter is required", 400);
        }
        JsonNode name = json.get("name");
        if (name == null || !name.isTextual()) {
            return errorMessage("name parameter is required", 400);
        }

        // TODO: Check if it's a real-time report.
        service.delete(project.asText(), name.asText());

        return JsonHelper.jsonObject().put("message", "successfully deleted");
    }

    public String createSelect(AggregationType aggType, String measure, String dimension) {

        if (measure == null) {
            if (aggType != AggregationType.COUNT)
                throw new IllegalArgumentException("either measure.expression or measure.field must be specified.");
        }

        StringBuilder builder = new StringBuilder();
        if (dimension != null)
            builder.append(" "+dimension+", ");

        switch (aggType) {
            case AVERAGE:
                return builder.append("avg(1) as value").toString();
            case MAXIMUM:
                return builder.append("max(1) as value").toString();
            case MINIMUM:
                return builder.append("min(1) as value").toString();
            case COUNT:
                return builder.append("count(1) as value").toString();
            case SUM:
                return builder.append("sum(1) as value").toString();
            case APPROXIMATE_UNIQUE:
                return builder.append("approx_distinct(1) as value").toString();
            case VARIANCE:
                return builder.append("variance(1) as value").toString();
            case POPULATION_VARIANCE:
                return builder.append("variance(1) as value").toString();
            case STANDARD_DEVIATION:
                return builder.append("stddev(1) as value").toString();
            default:
                throw new IllegalArgumentException("aggregation type couldn't found.");
        }
    }

    public static class RealTimeQuery {
        public final String project;
        public final String name;
        public final String filter;
        public final boolean aggregate;
        public final Instant dateStart;
        public final Instant dateEnd;

        @JsonCreator
        public RealTimeQuery(@JsonProperty("project") String project,
                             @JsonProperty("name") String name,
                             @JsonProperty("filter") String filter,
                             @JsonProperty("aggregate") boolean aggregate,
                             @JsonProperty("date_start") String dateStart,
                             @JsonProperty("date_end") String dateEnd) {
            this.project = project;
            this.name = name;
            this.filter = filter;
            this.aggregate = aggregate;
            this.dateStart = dateStart!=null ? Instant.parse(dateStart) : null;
            this.dateEnd = dateEnd !=null ? Instant.parse(dateEnd) : null;
        }
    }

    /*
     * Taken from http://stackoverflow.com/a/1657250/689144
     */
    public static String toSlug(String input) {
        String nowhitespace = WHITESPACE.matcher(input).replaceAll("_");
        String normalized = Normalizer.normalize(nowhitespace, Normalizer.Form.NFD);
        String slug = NONLATIN.matcher(normalized).replaceAll("");
        return slug.toLowerCase(Locale.ENGLISH);
    }
}
