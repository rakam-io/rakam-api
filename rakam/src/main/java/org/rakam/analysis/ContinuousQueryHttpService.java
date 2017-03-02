package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ProjectHttpService.Collection;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.SuccessMessage;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.Boolean.TRUE;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.MASTER_KEY;

@Path("/continuous-query")
@Api(value = "/continuous-query", nickname = "continuousQuery", description = "Continuous Query", tags = "continuous-query")
public class ContinuousQueryHttpService extends HttpService {
    private final ContinuousQueryService service;
    private final QueryExecutorService queryExecutorService;
    private final QueryHttpService queryHttpService;

    @Inject
    public ContinuousQueryHttpService(ContinuousQueryService service, QueryHttpService queryHttpService, QueryExecutorService queryExecutorService) {
        this.service = service;
        this.queryExecutorService = queryExecutorService;
        this.queryHttpService = queryHttpService;
    }

    public static final String PARTITION_KEY_INVALID = "Partition keys are not valid.";

    @JsonRequest
    @ApiOperation(value = "Create stream", authorizations = @Authorization(value = "master_key"), notes =
            "Creates a new continuous query for specified SQL query.\n" +
            "Rakam will process data in batches keep the result of query in-memory all the time.\n" +
            "Compared to reports, continuous queries continuously aggregate the data on the fly and the result is always available either in-memory or disk.")
    @ApiResponses(value = {@ApiResponse(code = 400, message = PARTITION_KEY_INVALID)})
    @Path("/create")
    public CompletableFuture<SuccessMessage> createQuery(@Named("project") String project, @ApiParam("continuous_query") ContinuousQuery report, @ApiParam(value = "replay", required = false) Boolean replay) {
        if (service.test(project, report.query)) {
            CompletableFuture<SuccessMessage> err = new CompletableFuture<>();
            // TODO: more readable message is needed.
            err.completeExceptionally(new RakamException("Query is not valid.", BAD_REQUEST));
        }

        CompletableFuture<List<SchemaField>> schemaFuture = queryExecutorService.metadata(project, report.query);
        return schemaFuture.thenApply(schema -> {
            if (report.partitionKeys.stream().filter(key -> !schema.stream().anyMatch(a -> a.getName().equals(key))).findAny().isPresent()) {
                throw new RakamException(PARTITION_KEY_INVALID, BAD_REQUEST);
            }
            try {
                QueryResult f = service.create(project, report, TRUE.equals(replay)).getResult().join();
                return SuccessMessage.map(f);
            } catch (IllegalArgumentException e) {
                throw new RakamException(e.getMessage(), BAD_REQUEST);
            }
        });
    }

    @JsonRequest
    @ApiOperation(value = "List queries", authorizations = @Authorization(value = "read_key"))

    @Path("/list")
    public List<ContinuousQuery> listQueries(@Named("project") String project) {
        return service.list(project);
    }

    @JsonRequest
    @ApiOperation(value = "Get query schema", authorizations = @Authorization(value = "read_key"))
    @Path("/schema")
    public List<Collection> getSchemaOfQuery(@Named("project") String project,
                                             @ApiParam(value = "names", required = false) List<String> names) {
        Map<String, List<SchemaField>> schemas = service.getSchemas(project);
        if (schemas == null) {
            throw new RakamException("Project does not exist", HttpResponseStatus.NOT_FOUND);
        }
        Stream<Collection> collectionStream = schemas.entrySet().stream()
                .map(entry -> new Collection(entry.getKey(), entry.getValue()));
        if (names != null) {
            collectionStream = collectionStream.filter(a -> names.contains(a.name));
        }
        return collectionStream.collect(Collectors.toList());
    }

    @JsonRequest
    @ApiOperation(value = "Delete stream", authorizations = @Authorization(value = "master_key"))

    @Path("/delete")
    public CompletableFuture<SuccessMessage> deleteQuery(@Named("project") String project,
                                                       @ApiParam("table_name") String tableName) {
        return service.delete(project, tableName).thenApply(success -> {
            if (success) {
                return SuccessMessage.success();
            } else {
                throw new RakamException("Error while deleting.", INTERNAL_SERVER_ERROR);
            }
        });
    }

    @ApiOperation(value = "Delete stream", authorizations = @Authorization(value = "master_key"))
    @Path("/refresh")
    @Consumes("text/event-stream")
    @GET
    @IgnoreApi
    public void refreshQuery(RakamHttpRequest request) {
        queryHttpService.handleServerSentQueryExecution(request, RefreshQuery.class,
                (project, q) -> service.refresh(project, q.table_name),
                MASTER_KEY, false);
    }

    public static class RefreshQuery {
        public final String table_name;

        @JsonCreator
        public RefreshQuery(@ApiParam("table_name") String table_name) {
            this.table_name = table_name;
        }
    }

    @JsonRequest
    @ApiOperation(value = "Test continuous query", authorizations = @Authorization(value = "read_key"))
    @Path("/test")
    public boolean testQuery(@Named("project") String project, @ApiParam("query") String query) {
        return service.test(project, query);
    }

    @JsonRequest
    @ApiOperation(value = "Get continuous query", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public ContinuousQuery getQuery(@Named("project") String project, @ApiParam("table_name") String tableName) {
        return service.get(project, tableName);
    }
}
