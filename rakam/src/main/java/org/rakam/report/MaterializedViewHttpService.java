package org.rakam.report;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import io.netty.channel.EventLoopGroup;
import org.rakam.collection.SchemaField;
import org.rakam.config.ForHttpServer;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonHelper;
import org.rakam.util.json.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.rakam.report.QueryHttpService.handleQueryExecution;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.util.JsonHelper.encode;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 01:14.
 */
@Path("/materialized-view")
@Api(value = "/materialized-view", description = "Materialized View", tags = "materialized-view")
public class MaterializedViewHttpService extends HttpService {
    private final MaterializedViewService service;
    private EventLoopGroup eventLoopGroup;

    @Inject
    public MaterializedViewHttpService(MaterializedViewService service) {
        this.service = service;
    }

    /**
     *     curl 'http://localhost:9999/materialized-view/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @ApiOperation(value = "Get lists of the materialized views")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/list")
    public Object list(@ApiParam(name="project", required = true) String project) {
        if (project == null) {
            return errorMessage("project parameter is required", 400);
        }

        return service.list(project);
    }

    /**
     *     curl 'http://localhost:9999/materialized-view/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @ApiOperation(value = "Get schemas of the materialized views")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/schema")
    public Object schema(@ApiParam(name="project", required = true) String project) {
        return new JsonResponse() {
            @JsonProperty("materialized-views")
            public final List views = service.getSchemas(project).entrySet().stream()
                    // ignore system tables
                    .filter(entry -> !entry.getKey().startsWith("_"))
                    .map(entry -> new JsonResponse() {
                        public final String name = entry.getKey();
                        public final List<SchemaField> fields = entry.getValue();
                    }).collect(Collectors.toList());
        };
    }

    /**
     * Creates a new materialized view for specified SQL query.
     * materialized views allow you to execute batch queries over the data-set.
     * Rakam caches the materialized view result and serve the cached data when you request.
     * You can also trigger an update using using '/view/update' endpoint.
     * This feature is similar to MATERIALIZED VIEWS in RDBMSs.
     *
     * @apiParamExample {json} Request-Example:
     *     {"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}
     *
     * @apiExample {curl} Example usage:
     *     curl 'http://localhost:9999/materialized-view/create' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @JsonRequest
    @ApiOperation(value = "Create new materialized view")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/create")
    public JsonResponse create(@ParamBody MaterializedView query) {
        service.create(query);
        return new JsonResponse() {
            public final boolean success = true;
        };
    }

    @JsonRequest
    @ApiOperation(value = "Delete materialized view")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/delete")
    public CompletableFuture<JsonResponse> delete(@ApiParam(name="project", required = true) String project,
                                                  @ApiParam(name="name", required = true) String name) {
        return service.delete(project, name)
                .thenApply(result -> new JsonResponse() {
                    public final boolean success = result.getError() == null;
                });
    }

    /**
     * Invalidate previous cached data, executes the materialized view query and caches it.
     * This feature is similar to UPDATE MATERIALIZED VIEWS in RDBMSs.
     *
     *     curl 'http://localhost:9999/materialized-view/update' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits"}'
     */
    @GET
    @Path("/update")
    @ApiOperation(value = "Update materialized view")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    public void update(RakamHttpRequest request) {
        RakamHttpRequest.StreamResponse response = request.streamResponse();

        Map<String, String> params = request.params().entrySet().stream()
                .collect(Collectors.toMap(k -> k.getKey(), k -> k.getValue().get(0)));
        MaterializedViewRequest query = JsonHelper.convert(params, MaterializedViewRequest.class);

        if(query.project == null || query.name == null) {
            response.send("result", encode(errorMessage("request is invalid", 400))).end();
        }

        QueryExecution update = service.update(service.get(query.project, query.name));
        handleQueryExecution(eventLoopGroup, response, update);
    }

    public static class MaterializedViewRequest {
        public final String project;
        public final String name;

        public MaterializedViewRequest(String project, String name) {
            this.project = project;
            this.name = name;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get materialized view")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/get")
    public Object get(@ApiParam(name="project", required = true) String project,
                      @ApiParam(name="name", required = true) String name) {
        return service.get(project, name);
    }

    @Inject
    public void setWorkerGroup(@ForHttpServer EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }
}