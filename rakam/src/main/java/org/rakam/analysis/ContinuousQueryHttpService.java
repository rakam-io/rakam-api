package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.RakamException;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.rakam.server.http.HttpServer.errorMessage;

/**
 * Created by buremba <Burak Emre Kabakcı> on 01/04/15 07:30.
 */
@Path("/continuous-query")
public class ContinuousQueryHttpService extends HttpService {
    private final ContinuousQueryService service;

    @Inject
    public ContinuousQueryHttpService(ContinuousQueryService service) {
        this.service = service;
    }

    /**
     * @api {post} /continuous-query/create Create new continuous query
     * @apiVersion 0.1.0
     * @apiName CreateContinuousQuery
     * @apiGroup continuous-query
     * @apiDescription Creates a new continuous query for specified SQL query.
     * Rakam will process data in batches keep the result of query in-memory all the time.
     * Compared to reports, continuous queries continuously aggregate the data on the fly and the result is always available either in-memory or disk.
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
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/reports/add/view' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @JsonRequest
    @Path("/create")
    public CompletableFuture<JsonResponse> create(ContinuousQuery report) {
        CompletableFuture<QueryResult> f;
        try {
            f = service.create(report);
        } catch (IllegalArgumentException e) {
            CompletableFuture<JsonResponse> err = new CompletableFuture<>();
            err.completeExceptionally(new RakamException(e.getMessage(), 400));
            return err;
        }
        return f.thenApply(result -> new JsonResponse() {
            public final boolean success = result.getError() == null;
            public final String error = result.getError().message;
        });
    }

    /**
     * @api {post} /continuous-query/list Lists continuous queries
     * @apiVersion 0.1.0
     * @apiName ListContinuousQueries
     * @apiGroup continuous-query
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
     * @api {post} /continuous-query/schema Get schemas of the materialized views
     * @apiVersion 0.1.0
     * @apiName GetSchemaOfMaterializedViews
     * @apiGroup materialized view
     * @apiDescription Returns lists of schemas of the materialized views created for the project.
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
     *     curl 'http://localhost:9999/continuous-query/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @Path("/schema")
    public Object schema(JsonNode json) {
        JsonNode project = json.get("project");
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }

        return new JsonResponse() {
            @JsonProperty("continuous-queries")
            public final List views = service.getSchemas(project.asText()).entrySet().stream()
                    // ignore system tables
                    .filter(entry -> !entry.getKey().startsWith("_"))
                    .map(entry -> new JsonResponse() {
                        public final String name = entry.getKey();
                        public final List<SchemaField> fields = entry.getValue();
                    }).collect(Collectors.toList());
        };
    }

    /**
     * @api {post} /continuous-query/delete Delete continuous query
     * @apiVersion 0.1.0
     * @apiName DeleteContinuousQueries
     * @apiGroup continuous-query
     * @apiDescription Deletes continuous query metadata and data.
     *
     * @apiError Project does not exist.
     *
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 500 Internal Server Error
     *     {"success": false, "message": "Project does not exists"}
     *
     * @apiParam {String} project   Project tracker code
     * @apiParam {String} name   Name
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/continuous-query/delete' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId", "name": "name"}'
     */
    @JsonRequest
    @Path("/delete")
    public Object delete(ContinuousQueryRequest query) {
        return service.delete(query.project, query.name).thenApply(result -> new JsonResponse() {
            public final boolean success = result.getError() == null;
            public final String error = result.getError().message;
        });
    }

    public static class ContinuousQueryRequest {
        public final String project;
        public final String name;

        public ContinuousQueryRequest(String project, String name) {
            this.project = project;
            this.name = name;
        }
    }
}
