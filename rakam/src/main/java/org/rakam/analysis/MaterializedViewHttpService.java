package org.rakam.analysis;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
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
import org.rakam.util.SuccessMessage;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

@Path("/materialized-view")
@Api(value = "/materialized-view", nickname = "materializedView", description = "Materialized View", tags = "materialized-view")
public class MaterializedViewHttpService extends HttpService {
    private final MaterializedViewService service;
    private final QueryHttpService queryService;

    @Inject
    public MaterializedViewHttpService(MaterializedViewService service, QueryHttpService queryService) {
        this.service = service;
        this.queryService = queryService;
    }

    @JsonRequest
    @ApiOperation(value = "List views", authorizations = @Authorization(value = "read_key"))

    @Path("/list")
    public List<MaterializedView> listViews(@javax.inject.Named("project") String project) {
        return service.list(project);
    }

    @JsonRequest
    @ApiOperation(value = "Get schemas", authorizations = @Authorization(value = "read_key"))

    @Path("/schema")
    public List<MaterializedViewSchema> getSchemaOfView(@javax.inject.Named("project") String project,
                                                                  @ApiParam(value = "names", required = false) List<String> tableNames) {
        return service.getSchemas(project, Optional.ofNullable(tableNames)).entrySet().stream()
                .map(entry -> new MaterializedViewSchema(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public static class MaterializedViewSchema {
        public final String name;
        public final List<SchemaField> fields;

        public MaterializedViewSchema(String name, List<SchemaField> fields) {
            this.name = name;
            this.fields = fields;
        }

    }

    /**
     * Creates a new materialized view for specified SQL query.
     * materialized views allow you to execute batch queries over the data-set.
     * Rakam caches the materialized view result and serve the cached data when you request.
     * You can also trigger an update using using '/view/update' endpoint.
     * This feature is similar to MATERIALIZED VIEWS in RDBMSs.
     * <p>
     * curl 'http://localhost:9999/materialized-view/create' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Yearly Visits", "query": "SELECT year(time), count(1) from visits GROUP BY 1"}'
     *
     * @param query materialized view query
     * @return the status
     */
    @JsonRequest
    @ApiOperation(value = "Create view", authorizations = @Authorization(value = "master_key"))

    @Path("/create")
    public CompletableFuture<SuccessMessage> createView(@javax.inject.Named("project") String project, @BodyParam MaterializedView query) {
        return service.create(project, query).thenApply(res -> SuccessMessage.success());
    }

    @JsonRequest
    @ApiOperation(value = "Delete materialized view", authorizations = @Authorization(value = "master_key"))

    @Path("/delete")
    public CompletableFuture<SuccessMessage> deleteView(@javax.inject.Named("project") String project,
                                                  @ApiParam("table_name") String name) {
        return service.delete(project, name)
                .thenApply(result -> {
                    if(result.getError() == null) {
                        return SuccessMessage.success();
                    } else {
                        throw new RakamException(result.getError().message, INTERNAL_SERVER_ERROR);
                    }
                });
    }

    @GET
    @Consumes("text/event-stream")
    @Path("/update")
    @ApiOperation(value = "Update view", authorizations = @Authorization(value = "master_key"), notes = "Invalidate previous cached data, executes the materialized view query and caches it.\n" +
            "This feature is similar to UPDATE MATERIALIZED VIEWS in RDBMSs.")
    @IgnoreApi
    public void update(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, MaterializedViewRequest.class,
                (project, query) -> {
                    QueryExecution execution = service.lockAndUpdateView(project, service.get(project, query.name)).queryExecution;
                    if (execution == null) {
                        QueryResult result = QueryResult.errorResult(new QueryError("There is another process that updates materialized view", null, null, null, null));
                        return QueryExecution.completedQueryExecution(null, result);
                    }
                    return execution;
                });
    }

    public static class MaterializedViewRequest {
        public final String name;

        public MaterializedViewRequest(String name) {
            this.name = name;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get view", authorizations = @Authorization(value = "read_key"))

    @Path("/get")
    public MaterializedView getView(@javax.inject.Named("project") String project,
                                @ApiParam("table_name") String tableName) {
        return service.get(project, tableName);
    }

}