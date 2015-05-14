package org.rakam.analysis;

import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.ws.rs.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 01/04/15 07:30.
 */
@Path("/continuous-query")
@Api(value = "/continuous-query", description = "Continuous Query", tags = "continuous-query")
public class ContinuousQueryHttpService extends HttpService {
    private final ContinuousQueryService service;

    @Inject
    public ContinuousQueryHttpService(ContinuousQueryService service) {
        this.service = service;
    }

    /**
     * Creates a new continuous query for specified SQL query.
     * Rakam will process data in batches keep the result of query in-memory all the time.
     * Compared to reports, continuous queries continuously aggregate the data on the fly and the result is always available either in-memory or disk.
     *
     * curl 'http://localhost:9999/reports/add/view' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     */
    @JsonRequest
    @ApiOperation(value = "Create realtime report")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/create")
    public CompletableFuture<JsonResponse> create(@ParamBody ContinuousQuery report) {
        CompletableFuture<QueryResult> f;
        try {
            f = service.create(report);
        } catch (IllegalArgumentException e) {
            CompletableFuture<JsonResponse> err = new CompletableFuture<>();
            err.completeExceptionally(new RakamException(e.getMessage(), 400));
            return err;
        }
        return f.thenApply(JsonResponse::map);
    }

    /**
     *     curl 'http://localhost:9999/reports/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @ApiOperation(value = "Create realtime report")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/list")
    public Object list(@ApiParam(name="project", required = true) String project) {
        return service.list(project);
    }

    /**
     *     curl 'http://localhost:9999/continuous-query/execute' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId"}'
     */
    @JsonRequest
    @ApiOperation(value = "Create realtime report")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/schema")
    public List<Collection> schema(@ApiParam(name="project", required = true) String project) {
        return service.getSchemas(project).entrySet().stream()
                    // ignore system tables
                    .filter(entry -> !entry.getKey().startsWith("_"))
                    .map(entry -> new Collection(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
    }

    public static class Collection {
        public final String name;
        public final List<SchemaField> fields;

        public Collection(String name, List<SchemaField> fields) {
            this.name = name;
            this.fields = fields;
        }
    }

    /**
     *     curl 'http://localhost:9999/continuous-query/delete' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{ "project": "projectId", "name": "name"}'
     */
    @JsonRequest
    @ApiOperation(value = "Delete realtime report")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/delete")
    public Object delete(@ApiParam(name="project", required = true) String project,
                         @ApiParam(name="name", required = true) String name) {
        return service.delete(project, name).thenApply(result -> {
            if(result.isFailed()) {
                return JsonResponse.error(result.getError().message);
            }else {
                return JsonResponse.success();
            }
        });
    }
}
