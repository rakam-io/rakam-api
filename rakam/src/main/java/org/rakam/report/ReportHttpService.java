package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.config.ForHttpServer;
import org.rakam.plugin.Report;
import org.rakam.plugin.AbstractReportService;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/reports")
public class ReportHttpService extends HttpService {
    private final SqlParser sqlParser;
    private final AbstractReportService service;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public ReportHttpService(AbstractReportService service) {
        this.service = service;
        this.sqlParser = new SqlParser();
    }

    /**
     * @api {post} /reports/list Get lists of the reports
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
     *     curl 'http://localhost:9999/reports/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @Path("/list")
    public Object list(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }

        return service.list(project.asText());
    }

    /**
     * @api {post} /reports/create Create new report
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
     * @apiParam {String} name   Project tracker code
     * @apiParam {String} query   Project tracker code
     * @apiParam {Object} [options]  Additional information about the report
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/reports/create' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @Path("/create")
    public void create(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> data = request.params().get("data");
        if(data == null) {
            response.send("result", encode(errorMessage("request is invalid", 400))).end();
            return;
        }

        Report report = JsonHelper.read(data.get(0), Report.class);
        QueryExecution queryExecution = service.create(report);

        handleQueryExecution(eventLoopGroup, response, queryExecution);
    }


    /**
     * @api {post} /reports/delete Delete report
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
     *     curl 'http://localhost:9999/reports/delete' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @JsonRequest
    @Path("/delete")
    public CompletableFuture<JsonResponse> delete(ReportQuery report) {
        return service.delete(report.project, report.name)
                .thenApply(result -> new JsonResponse() {
                    public final boolean success = result.getError() == null;
                });
    }

    /**
     * @api {get} /reports/update Update report
     * @apiVersion 0.1.0
     * @apiName UpdateReportData
     * @apiGroup report
     * @apiDescription Invalidate previous cached data, executes the report query and caches it.
     * This feature is similar to UPDATE MATERIALIZED VIEWS in RDBMSs.
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
     *     curl 'http://localhost:9999/reports/update' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @Path("/update")
    public void update(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> project = request.params().get("project");
        List<String> name = request.params().get("name");
        if(project == null || name == null) {
            response.send("result", encode(errorMessage("request is invalid", 400))).end();
        }

        QueryExecution update = service.update(project.get(0), name.get(0));
        handleQueryExecution(eventLoopGroup, response, update);
    }

    /**
     * @api {post} /reports/get Get reports
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
     *     curl 'http://localhost:9999/reports/get' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @JsonRequest
    @Path("/get")
    public Object get(ReportQuery query) {
        return service.getReport(query.project, query.name);
    }

    /**
     * @api {post} /reports/execute Execute query
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
     *     curl 'http://localhost:9999/reports/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId", "limit": 100, "offset": 100, "filters": }'
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

        ObjectNode json;
        try {
            json = JsonHelper.readSafe(data.get(0));
        } catch (IOException e) {
            response.send("result", encode(errorMessage("json couldn't parsed", 400))).end();
            return;
        }

        JsonNode project = json.get("project");
        if (project == null || !project.isTextual()) {
            response.send("result", encode(errorMessage("project parameter is required", 400))).end();
            return;
        }

        JsonNode query = json.get("query");
        if (project == null || !project.isTextual()) {
            response.send("result", encode(errorMessage("query parameter is required", 400))).end();
            return;
        }

        Statement statement;
        try {
            statement = createStatement(query.asText());
        } catch (ParsingException e) {
            response.send("result", encode(errorMessage("unable to parse query: "+e.getErrorMessage(), 400))).end();
            return;
        }

        QueryExecution execute = service.execute(project.asText(), statement);
        handleQueryExecution(eventLoopGroup, response, execute);
    }

    public static void handleQueryExecution(EventLoopGroup eventLoopGroup, RakamHttpRequest.StreamResponse response, QueryExecution query) {
        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(query.isFinished()) {
                    QueryResult result = query.getResult().join();
                    if(result.isFailed()) {
                        response.send("result", JsonHelper.jsonObject()
                                .put("success", false)
                                .put("query", query.getQuery())
                                .put("message", result.getError().message)).end();
                    }else {
                        response.send("result", encode(JsonHelper.jsonObject()
                                .put("success", true)
                                .putPOJO("query", query.getQuery())
                                .putPOJO("result", result.getResult())
                                .putPOJO("metadata", result.getMetadata()))).end();
                    }
                }else {
                    response.send("stats", encode(query.currentStats()));
                    eventLoopGroup.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    // sqlParser is not thread-safe and this part of the code is not performance critical.
    private synchronized Statement createStatement(String query) {
        return sqlParser.createStatement(query);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
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