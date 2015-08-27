package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.config.ForHttpServer;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiImplicitParam;
import org.rakam.server.http.annotations.ApiImplicitParams;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.jsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 20:03.
 */
@Path("/query")
@Api(value = "/query", description = "Query module", tags = "query")
@Produces({"application/json"})
public class QueryHttpService extends HttpService {
    private final QueryExecutor executor;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public QueryHttpService(QueryExecutor executor) {
        this.executor = executor;
    }


    @Path("/execute")
    @JsonRequest
    public CompletableFuture<QueryResult> execute(ExecuteQuery query) {
        return executor.executeQuery(query.project, query.query, query.limit == null ? 5000 : query.limit).getResult();
    }

    /**
     * @api {post} /query/execute Execute query
     * @apiVersion 0.1.0
     * @apiName ExecuteQuery
     * @apiGroup query
     * @apiDescription Executes SQL Queries on the fly and returns the result directly to the user
     *
     * @apiError Project does not exist.
     * @apiError Query Error
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Error Message"}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {Number} offset   Offset results
     * @apiParam {Number} limit   Limit results
     * @apiParam {Object[]} [filters]  Predicate that will be applied to user data
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/report/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "offset": 100, "filters": }'
     */
    @GET
    @ApiImplicitParams({
            @ApiImplicitParam(name = "project", value = "User's name", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "offset", value = "User's email", required = false, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "id", value = "User ID", required = true, dataType = "long", paramType = "query")
    })
    @Consumes("text/event-stream")
    @Path("/execute")
    public void execute(RakamHttpRequest request) {
        handleServerSentQueryExecution(request, ExecuteQuery.class, query ->
                executor.executeQuery(query.project, query.query, query.limit == null ? 5000 : query.limit));
    }

    public <T> void handleServerSentQueryExecution(RakamHttpRequest request, Class<T> clazz, Function<T, QueryExecution> executerFunction) {
        if (!Objects.equals(request.headers().get(HttpHeaders.Names.ACCEPT), "text/event-stream")) {
            request.response("The endpoint only supports text/event-stream as Accept header", HttpResponseStatus.NOT_ACCEPTABLE).end();
            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse();
        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(HttpServer.errorMessage("data query parameter is required", 400))).end();
            return;
        }

        T query;
        try {
            query = JsonHelper.readSafe(data.get(0), clazz);
        } catch (IOException e) {
            response.send("result", encode(HttpServer.errorMessage("json couldn't parsed: "+e.getMessage(), 400))).end();
            return;
        }

        QueryExecution execute;
        try {
            execute = executerFunction.apply(query);
        } catch (Exception e) {
            response.send("result", encode(HttpServer.errorMessage("couldn't execute query: " + e.getMessage(), 400))).end();
            return;
        }
        handleServerSentQueryExecution(eventLoopGroup, response, execute);
    }

    private void handleServerSentQueryExecution(EventLoopGroup eventLoopGroup, RakamHttpRequest.StreamResponse response, QueryExecution query) {
        query.getResult().whenComplete((result, ex) -> {
            if(response.isClosed()) {
              query.kill();
            } else if (ex != null) {
                response.send("result", encode(jsonObject()
                        .put("success", false)
                        .put("query", query.getQuery())
                        .put("error", "Internal error"))).end();
            } else if (result.isFailed()) {
                response.send("result", encode(jsonObject()
                        .put("success", false)
                        .put("query", query.getQuery())
                        .put("error", result.getError().message))).end();
            } else {
                List<? extends SchemaField> metadata = result.getMetadata();

                response.send("result", encode(jsonObject()
                        .put("success", true)
                        .putPOJO("query", query.getQuery())
                        .putPOJO("result", result.getResult())
                        .putPOJO("metadata", metadata))).end();
            }
        });

        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(response.isClosed()) {
                    query.kill();
                } else if(!query.isFinished()) {
                    response.send("stats", encode(query.currentStats()));
                    eventLoopGroup.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public static class ExecuteQuery {
        public final String project;
        public final String query;
        public final Segment segment;
        public final Integer limit;

        @JsonCreator
        public ExecuteQuery(@JsonProperty("project") String project,
                            @JsonProperty("query") String query,
                            @JsonProperty("segment") Segment segment,
                            @JsonProperty("limit") Integer limit) {
            this.project = checkNotNull(project, "project is empty");
            this.query = checkNotNull(query, "query is empty");;
            this.segment = segment;
            if(limit !=null && limit > 5000) {
                throw new IllegalArgumentException("maximum value of limit is 5000");
            }
            this.limit = limit;

        }

        public static class Segment {
            public final String x;
            public final String category;
            public final String value;

            @JsonCreator
            public Segment(@JsonProperty("x") String x,
                           @JsonProperty("category") String category,
                           @JsonProperty("value") String value) {
                this.x = x;
                this.category = category;
                this.value = value;
            }
        }
    }

    /**
     * @api {post} /query/explain Explain Query
     * @apiVersion 0.1.0
     * @apiName ExplainQuery
     * @apiGroup query
     * @apiDescription Parses query and returns the dimensions and measures of the query
     *
     * @apiError Query couldn't parsed
     *
     * @apiParam {String} query   Sql query
     *
     * @apiParamExample {json} Request-Example:
     *     {"query": "SELECT year(time), count(1) from visits GROUP BY 1"}
     *
     * @apiSuccess (200) {String} project Project tracker code
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {
     *       "query": "SELECT year(time), count(1) from visits GROUP BY 1",
     *     }
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/query/explain' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @JsonRequest
    @Path("/explain")
    public Object explain(@ApiParam(name="query", value = "Query", required = true) String query) {

//        NamedQuery namedQuery = new NamedQuery(query);
//        namedQuery.parameters().forEach(param -> namedQuery.bind(param, FieldType.LONG, null));
//        String query = namedQuery.build();
        try {
            Query statement;
            try {
                statement = (Query) new SqlParser().createStatement(query);
            } catch (Exception e) {
//                return new ResponseQuery(null, null, null, Lists.newArrayList(namedQuery.parameters()));
                return new ResponseQuery(null, null, null, null);
            }
            QuerySpecification queryBody = (QuerySpecification) statement.getQueryBody();
//            Expression where = queryBody.getWhere().orElse(null);

            Function<Expression, Integer> mapper = item -> {
                if (item instanceof QualifiedNameReference) {
                    return findSelectIndex(queryBody.getSelect().getSelectItems(), item.toString()).orElse(null);
                } else if (item instanceof LongLiteral) {
                    return Ints.checkedCast(((LongLiteral) item).getValue());
                } else {
                    return null;
                }
            };

            List<GroupBy> groupBy = queryBody.getGroupBy().stream()
                    .map(item -> new GroupBy(mapper.apply(item), item.toString()))
                    .collect(Collectors.toList());

            List<Ordering> orderBy = queryBody.getOrderBy().stream().map(item ->
                    new Ordering(item.getOrdering(), mapper.apply(item.getSortKey()), item.getSortKey().toString()))
                    .collect(Collectors.toList());

            String limitStr = queryBody.getLimit().orElse(null);
            Long limit = null;
            if(limitStr != null) {
                try {
                    limit = Long.parseLong(limitStr);
                } catch (NumberFormatException e) {}
            }

            return new ResponseQuery(groupBy, orderBy, limit, null);
        } catch (ParsingException|ClassCastException e) {
            return JsonResponse.error(e.getMessage());
        }
    }

    private Optional<Integer> findSelectIndex(List<SelectItem> selectItems, String reference) {
        for (int i = 0; i < selectItems.size(); i++) {
            SelectItem selectItem = selectItems.get(i);
            if (selectItem instanceof SingleColumn) {
                SingleColumn selectItem1 = (SingleColumn) selectItem;
                Optional<String> alias = selectItem1.getAlias();
                if((alias.isPresent() && alias.get().equals(reference)) ||
                        selectItem1.getExpression().toString().equals(reference)) {
                    return Optional.of(i+1);
                }
            }
        }
        return Optional.empty();
    }

    public static class ResponseQuery {
        public final List<GroupBy> groupBy;
        public final List<Ordering> orderBy;
        public final Long limit;
        public final List<String> parameters;

        @JsonCreator
        public ResponseQuery(List<GroupBy> groupBy, List<Ordering> orderBy, Long limit, List<String> parameters) {
            this.groupBy = groupBy;
            this.orderBy = orderBy;
            this.limit = limit;
            this.parameters = parameters;
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
