package org.rakam.analysis;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Path("/continuous-query")
@Api(value = "/continuous-query", description = "Continuous Query", tags = "continuous-query")
public class ContinuousQueryHttpService extends HttpService {
    private final ContinuousQueryService service;
    private final QueryExecutorService queryExecutorService;

    @Inject
    public ContinuousQueryHttpService(ContinuousQueryService service, QueryExecutorService queryExecutorService) {
        this.service = service;
        this.queryExecutorService = queryExecutorService;
    }

    /**
     * Creates a new continuous query for specified SQL query.
     * Rakam will process data in batches keep the result of query in-memory all the time.
     * Compared to reports, continuous queries continuously aggregate the data on the fly and the result is always available either in-memory or disk.
     *
     * curl 'http://localhost:9999/reports/add/view' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     * @param report continuous query report
     * @return a future that contains the operation status
     */
    @JsonRequest
    @ApiOperation(value = "Create stream", authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/create")
    public CompletableFuture<JsonResponse> create(@ParamBody ContinuousQuery report) {
        if(service.test(report.project, report.query)) {
            CompletableFuture<JsonResponse> err = new CompletableFuture<>();
            // TODO: more readable message is needed.
            err.completeExceptionally(new RakamException("Query is not valid.", HttpResponseStatus.BAD_REQUEST));
        }

        CompletableFuture<List<SchemaField>> schemaFuture = queryExecutorService.metadata(report.project, report.query);
        return schemaFuture.thenApply(schema -> {
            if (report.partitionKeys.stream().filter(key -> !schema.stream().anyMatch(a -> a.getName().equals(key))).findAny().isPresent()) {
                return JsonResponse.error("Partition keys are not valid.");
            }
            try {
                QueryResult f = service.create(report).join();
                return JsonResponse.map(f);
            } catch (IllegalArgumentException e) {
                return JsonResponse.error(e.getMessage());
            }
        });
    }

    @JsonRequest
    @ApiOperation(value = "List queries", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/list")
    public List<ContinuousQuery> listQueries(@ApiParam(name="project") String project) {
        return service.list(project);
    }

    @JsonRequest
    @ApiOperation(value = "Get query schema", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/schema")
    public List<Collection> schema(@ApiParam(name="project") String project,
                                   @ApiParam(name="names", required = false) List<String> names) {
        Map<String, List<SchemaField>> schemas = service.getSchemas(project);
        if(schemas == null) {
            throw new RakamException("project does not exist", HttpResponseStatus.NOT_FOUND);
        }
        Stream<Collection> collectionStream = schemas.entrySet().stream()
                // ignore system tables
//                    .filter(entry -> !entry.getKey().startsWith("_"))
                .map(entry -> new Collection(entry.getKey(), entry.getValue()));
        if(names != null) {
            collectionStream = collectionStream.filter(a -> names.contains(a.name));
        }
        return collectionStream.collect(Collectors.toList());
    }

    public static class Collection {
        public final String name;
        public final List<SchemaField> fields;

        public Collection(String name, List<SchemaField> fields) {
            this.name = name;
            this.fields = fields;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Delete stream", authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/delete")
    public CompletableFuture<JsonResponse> delete(@ApiParam(name="project") String project,
                         @ApiParam(name="name") String name) {
        return service.delete(project, name).thenApply(result -> {
            if(result) {
                return JsonResponse.error("Error while deleting.");
            }else {
                return JsonResponse.success();
            }
        });
    }

    @JsonRequest
    @ApiOperation(value = "Test continuous query", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/test")
    public boolean test(@ApiParam(name = "project") String project, @ApiParam(name = "query") String query) {
        return service.test(project, query);
    }

    @JsonRequest
    @ApiOperation(value = "Get continuous query", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/get")
    public ContinuousQuery get(@ApiParam(name = "project") String project, @ApiParam(name = "table_name") String tableName) {
        return service.get(project, tableName);
    }
}
