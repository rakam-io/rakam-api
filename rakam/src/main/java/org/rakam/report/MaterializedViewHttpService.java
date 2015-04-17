package org.rakam.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.netty.channel.EventLoopGroup;
import org.rakam.config.ForHttpServer;
import org.rakam.plugin.AbstractQueryService;
import org.rakam.plugin.MaterializedView;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.rakam.report.QueryHttpService.handleQueryExecution;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/materialized-view")
public class MaterializedViewHttpService extends HttpService {
    private final AbstractQueryService service;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public MaterializedViewHttpService(AbstractQueryService service) {
        this.service = service;
    }

    /**
     * @api {post} /materialized-view/list Get lists of the materialized views
     * @apiVersion 0.1.0
     * @apiName Listmaterialized views
     * @apiGroup materialized view
     * @apiDescription Returns lists of the materialized views created for the project.
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
     * @api {post} /materialized-view/create Create new materialized view
     * @apiVersion 0.1.0
     * @apiName Creatematerialized view
     * @apiGroup materialized view
     * @apiDescription Creates a new materialized view for specified SQL query.
     * materialized views allow you to execute batch queries over the data-set.
     * Rakam caches the materialized view result and serve the cached data when you request.
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
     * @apiParam {Object} [options]  Additional information about the materialized view
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
     * @api {post} /materialized-view/delete Delete materialized view
     * @apiVersion 0.1.0
     * @apiName Deletematerialized view
     * @apiGroup materialized view
     * @apiDescription Creates materialized view and cached data.
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
    public CompletableFuture<JsonResponse> delete(ReportQuery query) {
        return service.deleteMaterializedView(query.project, query.name)
                .thenApply(result -> new JsonResponse() {
                    public final boolean success = result.getError() == null;
                });
    }

    /**
     * @api {get} /materialized-view/update Update materialized view
     * @apiVersion 0.1.0
     * @apiName Updatematerialized viewData
     * @apiGroup materialized view
     * @apiDescription Invalidate previous cached data, executes the materialized view query and caches it.
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
     *     curl 'http://localhost:9999/materialized-view/update' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @Path("/update")
    public void update(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        List<String> project = request.params().get("project");
        List<String> name = request.params().get("name");
        if(project == null || name == null) {
            response.send("result", encode(errorMessage("request is invalid", 400))).end();
        }

        QueryExecution update = service.updateMaterializedView(project.get(0), name.get(0));
        handleQueryExecution(eventLoopGroup, response, update);
    }

    /**
     * @api {post} /materialized-view/get Get materialized views
     * @apiVersion 0.1.0
     * @apiName Getmaterialized viewMetadata
     * @apiGroup materialized view
     * @apiDescription Returns created materialized views.
     *
     * @apiError Project does not exist.
     * @apiError materialized view does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   materialized view name
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits"}
     *
     * @apiSuccess (200) {String} project Project tracker code
     * @apiSuccess (200) {String} name  materialized view name
     * @apiSuccess (200) {String} query  The SQL query of the materialized view
     * @apiSuccess (200) {Object} [options]  The options that are given when the materialized view is created
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