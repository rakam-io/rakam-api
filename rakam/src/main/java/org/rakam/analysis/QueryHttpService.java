package org.rakam.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.http.ForHttpServer;
import org.rakam.plugin.EventStore.CopyType;
import org.rakam.report.*;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.Response;
import org.rakam.server.http.annotations.*;
import org.rakam.util.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCEPT;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static java.util.Objects.requireNonNull;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.READ_KEY;
import static org.rakam.report.QueryExecutorService.DEFAULT_QUERY_RESULT_COUNT;
import static org.rakam.report.QueryExecutorService.MAX_QUERY_RESULT_LIMIT;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.server.http.HttpServer.returnError;
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.jsonObject;

@Path("/query")
@Api(value = "/query", nickname = "query", description = "Execute query", tags = "query")
@Produces({"application/json"})
public class QueryHttpService
        extends HttpService {
    private static final Logger LOGGER = Logger.get(QueryHttpService.class);
    private static Duration RETRY_DURATION = Duration.ofSeconds(600);
    private final QueryExecutorService executorService;
    private final ApiKeyService apiKeyService;
    private final SqlParser sqlParser = new SqlParser();
    private EventLoopGroup eventLoopGroup;

    @Inject
    public QueryHttpService(ApiKeyService apiKeyService, QueryExecutorService executorService) {
        this.executorService = executorService;
        this.apiKeyService = apiKeyService;
    }

    @Path("/execute")
    @ApiOperation(value = "Execute query on event data-set",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    public CompletableFuture<Response<QueryResult>> execute(
            RakamHttpRequest request,
            @Named("project") RequestContext context,
            @BodyParam QueryRequest query) {
        QueryExecution queryExecution = executorService.executeQuery(
                context,
                query.query,
                query.sample,
                query.defaultSchema,
                query.timezone,
                query.limit == null ? DEFAULT_QUERY_RESULT_COUNT : query.limit);

        return queryExecution
                .getResult().thenApply(result -> {
                    RakamClient.logEvent("run_query",
                            ImmutableMap.<String, Object>builder()
                                    .put("remote_address", Optional.ofNullable(request.getRemoteAddress()))
                                    .put("origin", Optional.ofNullable(request.headers().get(HttpHeaders.Names.ORIGIN)))
                                    .put("limit", Optional.ofNullable(query.limit))
                                    .put("timezone", Optional.ofNullable(query.timezone).map(e -> e.getId()))
                                    .put("sample_method", query.sample.map(e -> e.method.value()))
                                    .put("sample_percentage", query.sample.map(e -> e.percentage))
                                    .put("export_type", Optional.ofNullable(query.exportType)).build());

                    if (result.isFailed()) {
                        return Response.value(result, BAD_REQUEST);
                    }
                    return Response.ok(result);
                });
    }

    @Path("/export")
    @ApiOperation(value = "Export query results",
            authorizations = @Authorization(value = "read_key")
    )
    @IgnoreApi
    @JsonRequest
    public void export(RakamHttpRequest request, @Named("project") RequestContext context, @BodyParam QueryRequest query) {
        executorService.executeQuery(context, query.query,
                query.sample, query.defaultSchema,
                query.timezone,
                query.limit == null ? DEFAULT_QUERY_RESULT_COUNT : query.limit)

                .getResult().thenAccept(result -> {
            if (result.isFailed()) {
                returnError(request, result.getError().toString(), BAD_REQUEST);
                return;
            }
            byte[] bytes;
            switch (query.exportType) {
                case CSV:
                    try {
                        bytes = ExportUtil.exportAsCSV(result);
                    } catch (Exception e) {
                        LOGGER.error(e);
                        returnError(request, "Error while generating CSV.", INTERNAL_SERVER_ERROR);
                        return;
                    }
                    break;
                case AVRO:
                    try {
                        bytes = ExportUtil.exportAsAvro(result);
                    } catch (Exception e) {
                        LOGGER.error(e);
                        returnError(request, "Error while generating CSV.", INTERNAL_SERVER_ERROR);
                        return;
                    }
                    break;
                case JSON:
                    bytes = JsonHelper.encodeAsBytes(result.getResult());
                default:
                    throw new IllegalStateException();
            }

            request.response(bytes).end();
        });
    }

    @GET
    @Consumes("text/event-stream")
    @IgnoreApi
    @ApiOperation(value = "Analyze events asynchronously", request = QueryRequest.class,
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/execute")
    public void execute(RakamHttpRequest request, @QueryParam("read_key") String apiKey) {
        handleServerSentQueryExecution(request, QueryRequest.class, (project, query) ->
                executorService.executeQuery(new RequestContext(project, apiKey), query.query,
                        query.sample,
                        query.defaultSchema,
                        query.timezone,
                        query.limit == null ? DEFAULT_QUERY_RESULT_COUNT : query.limit));
    }

    public <T> void handleServerSentQueryExecution(RakamHttpRequest request, Class<T> clazz, BiFunction<String, T, QueryExecution> executorFunction, BiConsumer<T, QueryResult> exceptionCallback) {
        handleServerSentQueryExecution(request, clazz, executorFunction, READ_KEY, true, Optional.of(exceptionCallback));
    }

    public <T> void handleServerSentQueryExecution(RakamHttpRequest request, Class<T> clazz, BiFunction<String, T, QueryExecution> executorFunction) {
        handleServerSentQueryExecution(request, clazz, executorFunction, READ_KEY, true, Optional.empty());
    }

    public <T> void handleServerSentQueryExecution(RakamHttpRequest request, Class<T> clazz, BiFunction<String, T, QueryExecution> executorFunction, ApiKeyService.AccessKeyType keyType, boolean killOnConnectionClose, Optional<BiConsumer<T, QueryResult>> exceptionCallback) {
        if (!Objects.equals(request.headers().get(ACCEPT), "text/event-stream")) {
            request.response("The endpoint only supports text/event-stream as Accept header", HttpResponseStatus.NOT_ACCEPTABLE).end();
            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse(RETRY_DURATION);
        List<String> data;
        try {
            data = request.params().get("data");
        } catch (Exception e) {
            response.send("result", encode(errorMessage("enable to parse data parameter: " + e.getMessage(), BAD_REQUEST))).end();
            return;
        }

        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", BAD_REQUEST))).end();
            return;
        }

        T query;
        try {
            query = JsonHelper.readSafe(data.get(0), clazz);
        } catch (Exception e) {
            response.send("result", encode(errorMessage("JSON couldn't parsed: " + e.getMessage(), BAD_REQUEST))).end();
            return;
        }

        List<String> apiKey = request.params().get(keyType.getKey());
        if (apiKey == null || data.isEmpty()) {
            String message = keyType.getKey() + " query parameter is required";
            LogUtil.logException(request, new RakamException(message, BAD_REQUEST));
            response.send("result", encode(errorMessage(message, BAD_REQUEST))).end();
            return;
        }

        String project;
        try {
            project = apiKeyService.getProjectOfApiKey(apiKey.get(0), keyType);
        } catch (RakamException e) {
            if (e.getStatusCode() == FORBIDDEN) {
                response.send("result", encode(errorMessage(e.getMessage(), FORBIDDEN))).end();
            } else {
                response.send("result", encode(errorMessage(e.getMessage(), e.getStatusCode()))).end();
            }
            return;
        }

        QueryExecution execute;
        try {
            execute = executorFunction.apply(project, query);
        } catch (RakamException e) {
            LogUtil.logException(request, e);
            response.send("result", encode(errorMessage("Couldn't execute query: " + e.getMessage(), BAD_REQUEST))).end();
            return;
        } catch (Exception e) {
            LOGGER.error(e, "Error while executing query");
            response.send("result", encode(errorMessage("Couldn't execute query: Internal error", BAD_REQUEST))).end();
            return;
        }

        handleServerSentQueryExecutionInternal(response, execute, killOnConnectionClose,
                queryResult -> exceptionCallback.ifPresent(e -> e.accept(query, queryResult)));
    }

    private void handleServerSentQueryExecutionInternal(RakamHttpRequest.StreamResponse response, QueryExecution query, boolean killOnConnectionClose, Consumer<QueryResult> exceptionMapper) {
        if (query == null) {
            LOGGER.error("Query execution is null");
            response.send("result", encode(jsonObject()
                    .put("success", false)
                    .put("error", "Not running"))).end();
            return;
        }
        query.getResult().whenComplete((result, ex) -> {
            if (ex != null) {
                LOGGER.error(ex, "Error while executing query");
                response.send("result", encode(jsonObject()
                        .put("success", false)
                        .put("error", ex.getCause() instanceof RakamException ?
                                ex.getCause().getMessage() :
                                "Internal error"))).end();
            } else if (result.isFailed()) {
                if (!response.isClosed()) {
                    exceptionMapper.accept(result);
                }
                response.send("result", encode(jsonObject()
                        .put("success", false)
                        .putPOJO("error", result.getError())
                        .putPOJO("properties", result.getProperties())
                )).end();
            } else {
                List<? extends SchemaField> metadata = result.getMetadata();

                String encode = encode(jsonObject()
                        .put("success", true)
                        .putPOJO("properties", result.getProperties())
                        .putPOJO("result", result.getResult())
                        .putPOJO("metadata", metadata));
                response.send("result", encode).end();
            }
        });

        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                boolean finished = query.isFinished();
                if (response.isClosed() && !finished && killOnConnectionClose) {
                    query.kill();
                } else if (!finished) {
                    if (!response.isClosed()) {
                        QueryStats stats = query.currentStats();
                        response.send("stats", encode(stats));
                        if (stats.state == QueryStats.State.FINISHED) {
                            return;
                        }
                    }

                    eventLoopGroup.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    @JsonRequest
    @ApiOperation(value = "Explain query", authorizations = @Authorization(value = "read_key"))
    @Path("/explain")
    public ResponseQuery explain(@ApiParam(value = "query", description = "Query") String query) {
        try {
            Query statement = (Query) sqlParser.createStatement(query);

            Map<String, NodeLocation> map = statement.getWith().map(with -> {
                ImmutableMap.Builder<String, NodeLocation> builder = ImmutableMap.builder();
                with.getQueries().stream()
                        .forEach(withQuery ->
                                builder.put(withQuery.getName().getValue(), withQuery.getQuery().getLocation().orElse(null)));
                return builder.build();
            }).orElse(null);

            if (statement.getQueryBody() instanceof QuerySpecification) {
                return parseQuerySpecification((QuerySpecification) statement.getQueryBody(),
                        statement.getLimit(), statement.getOrderBy().map(v -> v.getSortItems()).orElse(null), map);
            } else if (statement.getQueryBody() instanceof Union) {
                Relation relation = ((Union) statement.getQueryBody()).getRelations().get(0);
                while (relation instanceof Union) {
                    relation = ((Union) relation).getRelations().get(0);
                }

                if (relation instanceof QuerySpecification) {
                    return parseQuerySpecification((QuerySpecification) relation,
                            statement.getLimit(), statement.getOrderBy().map(v -> v.getSortItems()).orElse(null), map);
                }
            }

            return new ResponseQuery(map,
                    statement.getQueryBody().getLocation().orElse(null),
                    ImmutableList.of(), ImmutableList.of(),
                    statement.getLimit().map(l -> Long.parseLong(l)).orElse(null));
        } catch (Throwable e) {
            return ResponseQuery.UNKNOWN;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Test query", authorizations = @Authorization(value = "read_key"))

    @Path("/metadata")
    public CompletableFuture<List<SchemaField>> metadata(@Named("project") RequestContext context, @ApiParam("query") String query) {
        return executorService.metadata(context, query);
    }

    private ResponseQuery parseQuerySpecification(QuerySpecification queryBody, Optional<String> limitOutside, List<SortItem> orderByOutside, Map<String, NodeLocation> with) {
        Function<Node, Integer> mapper = item -> {
            if (item instanceof GroupingElement) {
                return findSelectIndex(((GroupingElement) item).enumerateGroupingSets(), queryBody.getSelect().getSelectItems()).orElse(null);
            } else if (item instanceof LongLiteral) {
                return Ints.checkedCast(((LongLiteral) item).getValue());
            } else {
                return null;
            }
        };

        List<GroupBy> groupBy = queryBody.getGroupBy().map(value -> value.getGroupingElements().stream()
                .map(item -> new GroupBy(mapper.apply(item), item.enumerateGroupingSets().toString()))
                .collect(Collectors.toList())).orElse(ImmutableList.of());

        List<Ordering> orderBy;
        if (orderByOutside != null) {
            orderBy = orderByOutside.stream()
                    .map(e -> new Ordering(e.getOrdering(), mapper.apply(e.getSortKey()), e.getSortKey().toString()))
                    .collect(Collectors.toList());
        } else {
            orderBy = queryBody.getOrderBy().map(v -> v.getSortItems().stream().map(item ->
                    new Ordering(item.getOrdering(), mapper.apply(item.getSortKey()), item.getSortKey().toString()))
                    .collect(Collectors.toList())).orElse(ImmutableList.of());
        }

        String limitStr = limitOutside.orElse(queryBody.getLimit().orElse(null));
        Long limit = null;
        if (limitStr != null) {
            try {
                limit = Long.parseLong(limitStr);
            } catch (NumberFormatException e) {
            }
        }

        return new ResponseQuery(with, queryBody.getLocation().orElse(null), groupBy, orderBy, limit);
    }

    private Optional<Integer> findSelectIndex(List<Set<Expression>> items, List<SelectItem> selectItems) {
        if (items.size() == 1) {
            Set<Expression> item = items.get(0);
            if (item.size() == 1) {
                Expression next = item.iterator().next();
                if (next instanceof LongLiteral) {
                    return Optional.of(((int) ((LongLiteral) next).getValue()));
                } else if (next instanceof Identifier) {
                    for (int i = 0; i < selectItems.size(); i++) {
                        if (selectItems.get(i) instanceof SingleColumn) {
                            if (((SingleColumn) selectItems.get(i)).getExpression().equals(next)) {
                                return Optional.of(i + 1);
                            }
                        }
                    }
                }
                return Optional.empty();
            }
        }

        return Optional.empty();
    }

    public static class QueryRequest {
        public final String query;
        public final Integer limit;
        public final String defaultSchema;
        public final Optional<QuerySampling> sample;
        public final CopyType exportType;
        public final ZoneId timezone;

        @JsonCreator
        public QueryRequest(
                @ApiParam(value = "query", description = "SQL query that will be executed on data-set (SELECT count(*) from pageview)") String query,
                @ApiParam(value = "export_type", required = false, description = "Export data using different formats") CopyType exportType,
                @ApiParam(value = "sampling", required = false, description = "Optional parameter for specifying the sampling on source data") QuerySampling sample,
                @ApiParam(value = "default_schema", required = false, defaultValue = "collection", description = "The default schema of the query. If the schema is not defined, this schema will be used.") String defaultSchema,
                @ApiParam(value = "limit", required = false, description = "The maximum rows that can be returned from a query is 500K") Integer limit,
                @ApiParam(value = "timezone", required = false, description = "") ZoneId timezone) {
            this.query = requireNonNull(query, "query is empty").trim().replaceAll(";+$", "");
            if (limit != null && limit > MAX_QUERY_RESULT_LIMIT) {
                throw new IllegalArgumentException("Maximum value of limit is " + MAX_QUERY_RESULT_LIMIT);
            }
            this.exportType = exportType;
            this.defaultSchema = defaultSchema;
            this.sample = Optional.ofNullable(sample);
            this.limit = limit;
            this.timezone = timezone;
        }
    }

    public static class ResponseQuery {
        public static final ResponseQuery UNKNOWN = new ResponseQuery(null, null, ImmutableList.of(), ImmutableList.of(), null);

        public final Map<String, NodeLocation> with;
        public final List<GroupBy> groupBy;
        public final List<Ordering> orderBy;
        public final Long limit;
        public final NodeLocation queryLocation;

        @JsonCreator
        public ResponseQuery
                (Map<String, NodeLocation> with,
                 NodeLocation queryLocation,
                 List<GroupBy> groupBy,
                 List<Ordering> orderBy, Long limit) {
            this.with = with;
            this.queryLocation = queryLocation;
            this.groupBy = groupBy;
            this.orderBy = orderBy;
            this.limit = limit;
        }
    }

    public static class Ordering {
        public final SortItem.Ordering ordering;
        public final Integer index;
        public final String expression;

        @JsonCreator
        public Ordering(SortItem.Ordering ordering, Integer index, String expression) {
            this.ordering = ordering;
            this.index = index;
            this.expression = expression;
        }
    }

    public static class GroupBy {
        public final Integer index;
        public final String expression;

        @JsonCreator
        public GroupBy(Integer index, String expression) {
            this.index = index;
            this.expression = expression;
        }
    }
}
