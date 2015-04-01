package org.rakam.report;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ContinuousQuery;
import org.rakam.analysis.Report;
import org.rakam.analysis.query.QueryFormatter;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.config.ForHttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/reports")
public class ReportHttpService extends HttpService {
    private final ReportMetadataStore database;
    private final SqlParser sqlParser;
    private final PrestoQueryExecutor queryExecutor;
    private final PrestoConfig prestoConfig;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public ReportHttpService(ReportMetadataStore database,PrestoConfig prestoConfig, PrestoQueryExecutor queryExecutor) {
        this.database = database;
        this.prestoConfig = prestoConfig;
        this.queryExecutor = queryExecutor;
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

        return database.getReports(project.asText());
    }

    /**
     * @api {post} /reports/create/view Create new view
     * @apiVersion 0.1.0
     * @apiName CreateView
     * @apiGroup report
     * @apiDescription Creates a new view for specified SQL query.
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
     *     curl 'http://localhost:9999/reports/create/view' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @JsonRequest
    @Path("/create/view")
    public JsonNode addReport(Report report) {
        database.saveReport(report);
        return JsonHelper.jsonObject()
                .put("message", "Report successfully saved");
    }

    /**
     * @api {post} /reports/create/continuous-query Create new continuous query
     * @apiVersion 0.1.0
     * @apiName CreateContinuousQuery
     * @apiGroup report
     * @apiDescription Creates a new continuous query for specified SQL query.
     * Rakam will process data in batches keep the result of query in-memory all the time.
     * Compared to views, continuous queries continuously aggregate the data on the fly and the result is always available either in-memory or disk.
     *
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   Continuous query name
     * @apiParam {String} query   The query that will be continuously aggregated
     * @apiParam {String="stream","incremental"} strategy  Additional information about the report
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/reports/add/view' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @JsonRequest
    @Path("/create/continuous-query")
    public JsonNode createContinuousQuery(ContinuousQuery report) {
        database.createContinuousQuery(report);
        return JsonHelper.jsonObject()
                .put("message", "Materialized view successfully created");
    }

    /**
     * @api {post} /reports/metadata Get report metadata
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
     *     curl 'http://localhost:9999/reports/metadata' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @JsonRequest
    @Path("/metadata")
    public Object metadata(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }
        JsonNode name = json.get("name");
        if (name == null) {
            return errorMessage("name parameter is required", 400);
        }

        return database.getReport(project.asText(), name.asText());
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

        Query statement;
        try {
            statement = (Query) sqlParser.createStatement(query.asText());
        } catch (ParsingException e) {
            response.send("result", encode(errorMessage("unable to parse query: "+e.getErrorMessage(), 400))).end();
            return;
        }

        StringBuilder builder = new StringBuilder();
        // TODO: does cold storage supports schemas?
        new QueryFormatter(builder, node -> {
            QualifiedName prefix = node.getName().getPrefix().orElse(new QualifiedName(prestoConfig.getColdStorageConnector()));
            return prefix.getSuffix() + "." + project + "." + node.getName().getSuffix();
        }).process(statement, 0);

        String sqlQuery = builder.toString();

        PrestoQuery prestoQuery = queryExecutor.executeQuery(sqlQuery);
        handleQueryExecution(eventLoopGroup, response, prestoQuery);
    }

    public static void handleQueryExecution(EventLoopGroup eventLoopGroup, RakamHttpRequest.StreamResponse response, PrestoQuery prestoQuery) {
        eventLoopGroup.schedule(new Runnable() {
            @Override
            public void run() {
                if(prestoQuery.isFinished()) {
                    QueryResult result = prestoQuery.getResult().join();
                    if(result.isFailed()) {
                        response.send("result", JsonHelper.jsonObject()
                                .put("success", false)
                                .put("query", prestoQuery.getQuery())
                                .put("message", result.getError().message)).end();
                    }else {
                        response.send("result", encode(JsonHelper.jsonObject()
                                .put("success", true)
                                .putPOJO("query", prestoQuery.getQuery())
                                .putPOJO("result", result.getResult())
                                .putPOJO("metadata", result.getMetadata()))).end();
                    }
                }else {
                    response.send("stats", encode(prestoQuery.currentStats()));
                    eventLoopGroup.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

}