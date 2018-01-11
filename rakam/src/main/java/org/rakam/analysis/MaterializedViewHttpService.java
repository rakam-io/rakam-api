package org.rakam.analysis;

import org.rakam.analysis.MaterializedViewService.MaterializedViewExecution;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
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
public class MaterializedViewHttpService
        extends HttpService {
    private final MaterializedViewService service;
    private final QueryHttpService queryService;
    private final QueryExecutor executor;

    @Inject
    public MaterializedViewHttpService(MaterializedViewService service, QueryExecutor executor, QueryHttpService queryService) {
        this.service = service;
        this.queryService = queryService;
        this.executor = executor;
    }

    @JsonRequest
    @ApiOperation(value = "List views", authorizations = @Authorization(value = "read_key"))

    @Path("/list")
    public List<MaterializedView> listViews(@Named("project") RequestContext context) {
        return service.list(context.project);
    }

    @JsonRequest
    @ApiOperation(value = "Get schemas", authorizations = @Authorization(value = "read_key"))
    @Path("/schema")
    public List<MaterializedViewSchema> getSchemaOfView(@Named("project") RequestContext context,
                                                        @ApiParam(value = "names", required = false) List<String> tableNames) {
        return service.getSchemas(context, Optional.ofNullable(tableNames)).entrySet().stream()
                .map(entry -> new MaterializedViewSchema(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
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
    public CompletableFuture<SuccessMessage> createView(@Named("project") RequestContext context, @BodyParam MaterializedView query) {
        return service.create(context, query).thenApply(res -> SuccessMessage.success());
    }

    @JsonRequest
    @ApiOperation(value = "Delete materialized view", authorizations = @Authorization(value = "master_key"))

    @Path("/delete")
    public CompletableFuture<SuccessMessage> deleteView(@Named("project") RequestContext context,
                                                        @ApiParam("table_name") String name) {
        return service.delete(context, name)
                .thenApply(result -> {
                    if (result.getError() == null) {
                        return SuccessMessage.success();
                    } else {
                        throw new RakamException(result.getError().message, INTERNAL_SERVER_ERROR);
                    }
                });
    }

    @JsonRequest
    @ApiOperation(value = "Change materialized view", authorizations = @Authorization(value = "master_key"))

    @Path("/change")
    public SuccessMessage changeView(@Named("project") RequestContext context,
                                     @ApiParam("table_name") String tableName,
                                     @ApiParam("real_time") boolean realTime) {
        service.changeView(context.project, tableName, realTime);
        return SuccessMessage.success();
    }

    @GET
    @Consumes("text/event-stream")
    @Path("/update")
    @ApiOperation(value = "Update view", authorizations = @Authorization(value = "master_key"), notes = "Invalidate previous cached data, executes the materialized view query and caches it.\n" +
            "This feature is similar to UPDATE MATERIALIZED VIEWS in RDBMSs.")
    @IgnoreApi
    public void update(RakamHttpRequest request, @QueryParam("master_key") String apiKey) {
        queryService.handleServerSentQueryExecution(request, MaterializedViewRequest.class,
                (project, query) -> {
                    RequestContext context = new RequestContext(project, apiKey);
                    MaterializedViewExecution execution = service.lockAndUpdateView(context, service.get(project, query.name));
                    if (execution.materializedViewUpdateQuery == null) {
                        QueryResult result = QueryResult.errorResult(new QueryError("There is another process that updates materialized view", null, null, null, null));
                        return QueryExecution.completedQueryExecution(null, result);
                    }
                    return execution.materializedViewUpdateQuery.get();
                });
    }

    @JsonRequest
    @ApiOperation(value = "Get view", authorizations = @Authorization(value = "read_key"))

    @Path("/get")
    public MaterializedView getView(@Named("project") RequestContext context,
                                    @ApiParam("table_name") String tableName) {
        return service.get(context.project, tableName);
    }

    public static class MaterializedViewSchema {
        public final String name;
        public final List<SchemaField> fields;

        public MaterializedViewSchema(String name, List<SchemaField> fields) {
            this.name = name;
            this.fields = fields;
        }
    }

    public static class MaterializedViewRequest {
        public final String name;

        public MaterializedViewRequest(String name) {
            this.name = name;
        }
    }
}