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
import com.google.common.primitives.Ints;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.config.ForHttpServer;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
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

import static java.util.Objects.requireNonNull;
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.jsonObject;

@Path("/query")
@Api(value = "/query", description = "Query module", tags = {"event"})
@Produces({"application/json"})
public class QueryHttpService extends HttpService {
    private final QueryExecutor executor;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public QueryHttpService(QueryExecutor executor) {
        this.executor = executor;
    }


    @Path("/execute")
    @ApiOperation(value = "Analyze events",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    public CompletableFuture<QueryResult> execute(@ParamBody ExecuteQuery query) {
        return executor.executeQuery(query.project, query.query, query.limit == null ? 5000 : query.limit).getResult();
    }

    @GET
    @Consumes("text/event-stream")
    @ApiOperation(value = "Analyze events asynchronously", request = ExecuteQuery.class,
            authorizations = @Authorization(value = "read_key")
    )
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
        public final Integer limit;

        @JsonCreator
        public ExecuteQuery(@ApiParam(name = "project") String project,
                            @ApiParam(name = "query") String query,
                            @ApiParam(name = "limit", required = false) Integer limit) {
            this.project = requireNonNull(project, "project is empty");
            this.query = requireNonNull(query, "query is empty");;
            if(limit !=null && limit > 5000) {
                throw new IllegalArgumentException("maximum value of limit is 5000");
            }
            this.limit = limit;

        }
    }

    @JsonRequest
    @Path("/explain")
    public Object explain(@ApiParam(name="query", value = "Query", required = true) String query) {
        try {
            Query statement;
            try {
                statement = (Query) new SqlParser().createStatement(query);
            } catch (Exception e) {
                return new ResponseQuery(null, null, null, null);
            }
            QuerySpecification queryBody = (QuerySpecification) statement.getQueryBody();

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
