package org.rakam.ui;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.Path;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/04/15 22:43.
 */
@Path("/report")
public class ReportHttpService extends HttpService {

    private final JDBCReportMetadata metadata;

    @Inject
    public ReportHttpService(JDBCReportMetadata metadata) {
        this.metadata = metadata;
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
            return HttpServer.errorMessage("project parameter is required", 400);
        }

        return metadata.getReports(project.asText());
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
    public JsonResponse create(Report report) {
        metadata.save(report);
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
    public JsonResponse delete(ReportQuery report) {
        metadata.delete(report.project, report.name);

        return new JsonResponse() {
            public final boolean success = true;
        };
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
        return metadata.get(query.project, query.name);
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
