package org.rakam.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Union;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.annotations.ApiModelProperty;
import org.rakam.collection.SchemaField;
import org.rakam.http.ForHttpServer;
import org.rakam.plugin.EventStore.CopyType;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.ExportUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.LogUtil;

import javax.inject.Inject;
import javax.inject.Named;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCEPT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static java.util.Objects.requireNonNull;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.READ_KEY;
import static org.rakam.report.QueryExecutorService.MAX_QUERY_RESULT_LIMIT;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;
import static org.rakam.util.JsonHelper.jsonObject;

@Path("/query")
@Api(value = "/query", nickname = "query", description = "Execute query", tags = {"query"})
@Produces({"application/json"})
public class QueryHttpService
        extends HttpService
{
    private static final Logger LOGGER = Logger.get(QueryHttpService.class);
    private final QueryExecutorService executorService;
    private final ApiKeyService apiKeyService;
    private EventLoopGroup eventLoopGroup;
    private final SqlParser sqlParser = new SqlParser();

    @Inject
    public QueryHttpService(ApiKeyService apiKeyService, QueryExecutorService executorService)
    {
        this.executorService = executorService;
        this.apiKeyService = apiKeyService;
    }

    @Path("/execute")
    @ApiOperation(value = "Execute query on event data-set",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    public CompletableFuture<QueryResult> execute(
            @Named("project") String project,
            @BodyParam ExportQuery query)
    {
        return executorService.executeQuery(project, query.query, query.limit == null ? MAX_QUERY_RESULT_LIMIT : query.limit).getResult().thenApply(result -> {
            if (result.isFailed()) {
                throw new RakamException(result.getError().toString(), BAD_REQUEST);
            }
            return result;
        });
    }

    @Path("/export")
    @ApiOperation(value = "Export query results",
            authorizations = @Authorization(value = "read_key")
    )
    @IgnoreApi
    @JsonRequest
    public void export(RakamHttpRequest request, @Named("project") String project, @BodyParam ExportQuery query)
    {
        executorService.executeQuery(project, query.query, query.limit == null ? MAX_QUERY_RESULT_LIMIT : query.limit).getResult().thenAccept(result -> {
            if (result.isFailed()) {
                throw new RakamException(result.getError().toString(), BAD_REQUEST);
            }
            byte[] bytes;
            switch (query.exportType) {
                case CSV:
                    bytes = ExportUtil.exportAsCSV(result);
                    break;
                case AVRO:
                    bytes = ExportUtil.exportAsAvro(result);
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
    @ApiOperation(value = "Analyze events asynchronously", request = ExportQuery.class,
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/execute")
    public void execute(RakamHttpRequest request)
    {
        handleServerSentQueryExecution(request, ExportQuery.class, (project, query) ->
                executorService.executeQuery(project, query.query, query.limit == null ? MAX_QUERY_RESULT_LIMIT : query.limit));
    }

    public <T> void handleServerSentQueryExecution(RakamHttpRequest request, Class<T> clazz, BiFunction<String, T, QueryExecution> executorFunction)
    {
        handleServerSentQueryExecution(request, clazz, executorFunction, READ_KEY, true);
    }

    public <T> void handleServerSentQueryExecution(RakamHttpRequest request, Class<T> clazz, BiFunction<String, T, QueryExecution> executorFunction, ApiKeyService.AccessKeyType keyType, boolean killOnConnectionClose)
    {
        if (!Objects.equals(request.headers().get(ACCEPT), "text/event-stream")) {
            request.response("The endpoint only supports text/event-stream as Accept header", HttpResponseStatus.NOT_ACCEPTABLE).end();
            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse();
        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", BAD_REQUEST))).end();
            return;
        }

        T query;
        try {
            query = JsonHelper.readSafe(data.get(0), clazz);
        }
        catch (IOException e) {
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
        }
        catch (RakamException e) {
            if (e.getStatusCode() == FORBIDDEN) {
                response.send("result", encode(errorMessage(e.getMessage(), FORBIDDEN))).end();
            }
            else {
                response.send("result", encode(errorMessage(e.getMessage(), e.getStatusCode()))).end();
            }
            return;
        }

        QueryExecution execute;
        try {
            execute = executorFunction.apply(project, query);
        }
        catch (RakamException e) {
            LogUtil.logException(request, e);
            response.send("result", encode(errorMessage("Couldn't execute query: " + e.getMessage(), BAD_REQUEST))).end();
            return;
        }
        catch (Exception e) {
            LOGGER.error(e, "Error while executing query");
            response.send("result", encode(errorMessage("Couldn't execute query: Internal error", BAD_REQUEST))).end();
            return;
        }

        handleServerSentQueryExecution(request, execute, killOnConnectionClose);
    }

    public void handleServerSentQueryExecution(RakamHttpRequest request, QueryExecution query, boolean killOnConnectionClose)
    {
        RakamHttpRequest.StreamResponse response = request.streamResponse();
        if (query == null) {
            // TODO: custom message
            response.send("result", encode(jsonObject()
                    .put("success", false)
                    .put("query", query.getQuery())
                    .put("error", "Not running"))).end();
            return;
        }
        query.getResult().whenComplete((result, ex) -> {
            if (ex != null) {

                LOGGER.error(ex, "Error while executing query");
                response.send("result", encode(jsonObject()
                        .put("success", false)
                        .put("query", query.getQuery())
                        .put("error", ex.getCause() instanceof RakamException ?
                                ex.getCause().getMessage() :
                                "Internal error"))).end();
            }
            else if (result.isFailed()) {
                response.send("result", encode(jsonObject()
                        .put("success", false)
                        .put("query", query.getQuery())
                        .putPOJO("error", result.getError()))).end();
            }
            else {
                List<? extends SchemaField> metadata = result.getMetadata();

                response.send("result", encode(jsonObject()
                        .put("success", true)
                        .putPOJO("query", query.getQuery())
                        .putPOJO("properties", result.getProperties())
                        .putPOJO("result", result.getResult())
                        .putPOJO("metadata", metadata))).end();
            }
        });

        eventLoopGroup.schedule(new Runnable()
        {
            @Override
            public void run()
            {
                if (response.isClosed() && !query.isFinished() && killOnConnectionClose) {
                    query.kill();
                }
                else if (!query.isFinished()) {
                    if (!response.isClosed()) {
                        String encode = encode(query.currentStats());
                        response.send("stats", encode);
                    }
                    eventLoopGroup.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup)
    {
        this.eventLoopGroup = eventLoopGroup;
    }

    public static class ExportQuery
    {
        @ApiModelProperty(example = "SELECT 1", value = "SQL query that will be executed on data-set")
        public final String query;
        public final Integer limit;
        public final CopyType exportType;

        @JsonCreator
        public ExportQuery(
                @ApiParam("query") String query,
                @ApiParam("export_type") CopyType exportType,
                @ApiParam(value = "limit", required = false) Integer limit)
        {
            this.query = requireNonNull(query, "query is empty").trim().replaceAll(";+$", "");
            if (limit != null && limit > MAX_QUERY_RESULT_LIMIT) {
                throw new IllegalArgumentException("maximum value of limit is "+MAX_QUERY_RESULT_LIMIT);
            }
            this.exportType = exportType;
            this.limit = limit;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Explain query", authorizations = @Authorization(value = "read_key"))
    @Path("/explain")
    public ResponseQuery explain(@ApiParam(value = "query", description = "Query") String query)
    {
        try {
            Query statement;
            synchronized (sqlParser) {
                statement = (Query) sqlParser.createStatement(query);
            }

            if (statement.getQueryBody() instanceof QuerySpecification) {
                return parseQuerySpecification((QuerySpecification) statement.getQueryBody());
            }
            else if (statement.getQueryBody() instanceof Union) {
                Relation relation = ((Union) statement.getQueryBody()).getRelations().get(0);
                while (relation instanceof Union) {
                    relation = ((Union) relation).getRelations().get(0);
                }

                if (relation instanceof QuerySpecification) {
                    return parseQuerySpecification((QuerySpecification) relation);
                }
            }
            return new ResponseQuery(ImmutableList.of(), ImmutableList.of(),
                    statement.getLimit().map(l -> Long.parseLong(l)).orElse(null));
        }
        catch (Throwable e) {
            return ResponseQuery.UNKNOWN;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Test query", authorizations = @Authorization(value = "read_key"))

    @Path("/metadata")
    public CompletableFuture<List<SchemaField>> metadata(@javax.inject.Named("project") String project, @ApiParam("query") String query)
    {
        return executorService.metadata(project, query);
    }

    private ResponseQuery parseQuerySpecification(QuerySpecification queryBody)
    {
        Function<Node, Integer> mapper = item -> {
            if (item instanceof QualifiedNameReference) {
                return findSelectIndex(queryBody.getSelect().getSelectItems(), item.toString()).orElse(null);
            }
            else if (item instanceof LongLiteral) {
                return Ints.checkedCast(((LongLiteral) item).getValue());
            }
            else {
                return null;
            }
        };

        List<GroupBy> groupBy = queryBody.getGroupBy().map(value -> value.getGroupingElements().stream()
                .map(item -> new GroupBy(mapper.apply(item), item.toString()))
                .collect(Collectors.toList())).orElse(ImmutableList.of());

        List<Ordering> orderBy = queryBody.getOrderBy().stream().map(item ->
                new Ordering(item.getOrdering(), mapper.apply(item.getSortKey()), item.getSortKey().toString()))
                .collect(Collectors.toList());

        String limitStr = queryBody.getLimit().orElse(null);
        Long limit = null;
        if (limitStr != null) {
            try {
                limit = Long.parseLong(limitStr);
            }
            catch (NumberFormatException e) {
            }
        }

        return new ResponseQuery(groupBy, orderBy, limit);
    }

    private Optional<Integer> findSelectIndex(List<SelectItem> selectItems, String reference)
    {
        for (int i = 0; i < selectItems.size(); i++) {
            SelectItem selectItem = selectItems.get(i);
            if (selectItem instanceof SingleColumn) {
                SingleColumn selectItem1 = (SingleColumn) selectItem;
                Optional<String> alias = selectItem1.getAlias();
                if ((alias.isPresent() && alias.get().equals(reference)) ||
                        selectItem1.getExpression().toString().equals(reference)) {
                    return Optional.of(i + 1);
                }
            }
        }
        return Optional.empty();
    }

    public static class ResponseQuery
    {
        public static final ResponseQuery UNKNOWN = new ResponseQuery(ImmutableList.of(), ImmutableList.of(), null);

        public final List<GroupBy> groupBy;
        public final List<Ordering> orderBy;
        public final Long limit;

        @JsonCreator
        public ResponseQuery(List<GroupBy> groupBy, List<Ordering> orderBy, Long limit)
        {
            this.groupBy = groupBy;
            this.orderBy = orderBy;
            this.limit = limit;
        }
    }

    public static class Ordering
    {
        public final SortItem.Ordering ordering;
        public final Integer index;
        public final String expression;

        @JsonCreator
        public Ordering(SortItem.Ordering ordering, Integer index, String expression)
        {
            this.ordering = ordering;
            this.index = index;
            this.expression = expression;
        }
    }

    public static class GroupBy
    {
        public final Integer index;
        public final String expression;

        @JsonCreator
        public GroupBy(Integer index, String expression)
        {
            this.index = index;
            this.expression = expression;
        }
    }
}
