package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.FieldType;
import org.rakam.config.ForHttpServer;
import org.rakam.plugin.AbstractReportService;
import org.rakam.plugin.MaterializedView;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/04/15 22:43.
 */
@Path("report")
public class ViewHttpService extends HttpService {
    private final SqlParser sqlParser;
    private EventLoopGroup eventLoopGroup;
    private final AbstractReportService service;

    @Inject
    public ViewHttpService(AbstractReportService service) {
        this.service = service;
        this.sqlParser = new SqlParser();
    }

    /**
     * @api {post} /report/execute Execute query
     * @apiVersion 0.1.0
     * @apiName ExecuteQuery
     * @apiGroup event
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
    @Path("/execute")
    public void execute(RakamHttpRequest request) {
        if (!Objects.equals(request.headers().get(HttpHeaders.Names.ACCEPT), "text/event-stream")) {
            request.response("the response should accept text/event-stream", HttpResponseStatus.NOT_ACCEPTABLE).end();
            return;
        }

        RakamHttpRequest.StreamResponse response = request.streamResponse();
        List<String> data = request.params().get("data");
        if (data == null || data.isEmpty()) {
            response.send("result", encode(errorMessage("data query parameter is required", 400))).end();
            return;
        }

        ExecuteQuery query;
        try {
            query = JsonHelper.readSafe(data.get(0), ExecuteQuery.class);
        } catch (IOException e) {
            response.send("result", encode(errorMessage("json couldn't parsed", 400))).end();
            return;
        }

        if (query.project == null) {
            response.send("result", encode(errorMessage("project parameter is required", 400))).end();
            return;
        }

        if (query.query == null) {
            response.send("result", encode(errorMessage("query parameter is required", 400))).end();
            return;
        }

//        NamedQuery namedQuery = new NamedQuery(query.query);
//        if(query.bindings != null) {
//            query.bindings.forEach((bindName, binding) -> namedQuery.bind(bindName, binding.type, binding.value));
//        }
//        String sqlQuery = namedQuery.build();

        Statement statement;
        try {
            synchronized (sqlParser) {
                statement = sqlParser.createStatement(query.query);
            }
        } catch (ParsingException e) {
            response.send("result", encode(errorMessage("unable to parse query: "+e.getErrorMessage(), 400))).end();
            return;
        }

        QueryExecution execute = service.execute(query.project, statement);
        handleQueryExecution(eventLoopGroup, response, execute);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    static void handleQueryExecution(EventLoopGroup eventLoopGroup, RakamHttpRequest.StreamResponse response, QueryExecution query) {
        query.getResult().whenComplete((result, ex) -> {
            if (ex != null) {
                response.send("result", JsonHelper.jsonObject()
                        .put("success", false)
                        .put("query", query.getQuery())
                        .put("message", "Internal error")).end();
            } else if (result.isFailed()) {
                response.send("result", JsonHelper.jsonObject()
                        .put("success", false)
                        .put("query", query.getQuery())
                        .put("message", result.getError().message)).end();
            } else {
                response.send("result", encode(JsonHelper.jsonObject()
                        .put("success", true)
                        .putPOJO("query", query.getQuery())
                        .putPOJO("result", result.getResult())
                        .putPOJO("metadata", result.getMetadata()))).end();
            }
        });

        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(!query.isFinished()) {
                    response.send("stats", encode(query.currentStats()));
                    eventLoopGroup.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }, 500, TimeUnit.MILLISECONDS);
    }


    /**
     * @api {post} /materialized-view/list Get lists of the reports
     * @apiVersion 0.1.0
     * @apiName ListReports
     * @apiGroup report
     * @apiDescription Returns lists of the reports created for the project.
     *
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     *
     * @apiSuccess {String} firstname Firstname of the User.
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/materialized-view/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @Path("/list")
    public Object list(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }

        return service.listMaterializedViews(project.asText());
    }

    /**
     * @api {post} /materialized-view/create Create new report
     * @apiVersion 0.1.0
     * @apiName CreateReport
     * @apiGroup report
     * @apiDescription Creates a new report for specified SQL query.
     * Reports allow you to execute batch queries over the data-set.
     * Rakam caches the report result and serve the cached data when you request.
     * You can also trigger an update using using '/view/update' endpoint.
     * This feature is similar to MATERIALIZED VIEWS in RDBMSs.
     *
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} table_name   Table name
     * @apiParam {String} name   Project tracker code
     * @apiParam {String} query   Project tracker code
     * @apiParam {Object} [options]  Additional information about the report
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/materialized-view/create' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @JsonRequest
    @Path("/create")
    public JsonResponse create(MaterializedView query) {

//        NamedQuery namedQuery = new NamedQuery(query.query);
//        String sqlQuery = namedQuery.build();

//        Statement statement1 = (Statement) invokeParser("statement", query.query, org.rakam.sql.test.NamedParamParser::query);
//        String s = SqlFormatter.formatSql(statement1);

        service.create(query);
        return new JsonResponse() {
            public final boolean success = true;
        };
    }

    /**
     * @api {post} /materialized-view/delete Delete report
     * @apiVersion 0.1.0
     * @apiName DeleteReport
     * @apiGroup report
     * @apiDescription Creates report and cached data.
     *
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   Project tracker code
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits"}
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/materialized-view/delete' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @JsonRequest
    @Path("/delete")
    public CompletableFuture<JsonResponse> delete(ReportQuery report) {
        return service.deleteMaterializedView(report.project, report.name)
                .thenApply(result -> new JsonResponse() {
                    public final boolean success = result.getError() == null;
                });
    }

    /**
     * @api {post} /materialized-view/get Get reports
     * @apiVersion 0.1.0
     * @apiName GetReportMetadata
     * @apiGroup report
     * @apiDescription Returns created reports.
     *
     * @apiError Project does not exist.
     * @apiError Report does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   Report name
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits"}
     *
     * @apiSuccess (200) {String} project Project tracker code
     * @apiSuccess (200) {String} name  Report name
     * @apiSuccess (200) {String} query  The SQL query of the report
     * @apiSuccess (200) {Object} [options]  The options that are given when the report is created
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {
     *       "project": "projectId",
     *       "name": "Yearly Visits",
     *       "query": "SELECT year(time), count(1) from visits GROUP BY 1",
     *     }
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/materialized-view/get' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @JsonRequest
    @Path("/get")
    public Object get(ReportQuery query) {
        return service.getMaterializedView(query.project, query.name);
    }

    public static class ExecuteQuery {
        public final String project;
        public final String query;
        public final Map<String, Binding> bindings;

        @JsonCreator
        public ExecuteQuery(@JsonProperty("project") String project,
                            @JsonProperty("query") String query,
                            @JsonProperty("bindings") Map<String, Binding> bindings) {
            this.project = project;
            this.query = query;
            this.bindings = bindings;
        }

        public static class Binding {
            public final FieldType type;
            public final Object value;

            @JsonCreator
            public Binding(@JsonProperty("type") FieldType type,
                           @JsonProperty("value") Object value) {
                this.type = type;
                this.value = value;
            }
        }
    }

    public static class ReportQuery {
        public final String project;
        public final String name;

        public ReportQuery(String project, String name) {
            this.project = project;
            this.name = name;
        }
    }
}
