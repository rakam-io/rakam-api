package org.rakam.analysis.realtime;

import com.google.inject.Singleton;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.realtime.RealtimeService.RealTimeQueryResult;
import org.rakam.report.realtime.RealTimeReport;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.util.Objects.requireNonNull;

@Singleton
@Api(value = "/realtime", nickname = "realtime", description = "Realtime module", tags = "realtime")
@Path("/realtime")
public class RealTimeHttpService
        extends HttpService {
    private final RealtimeService realtimeService;

    @Inject
    public RealTimeHttpService(RealtimeService realtimeService) {
        this.realtimeService = requireNonNull(realtimeService, "realtimeService is null");
    }

    /**
     * Creates real-time report using continuous queries.
     * This module adds a new attribute called 'time' to events, it's simply a unix epoch that represents the seconds the event is occurred.
     * Continuous query continuously aggregates 'time' column and
     * real-time module executes queries on continuous query table similar to 'select count from stream_count where time &gt; now() - interval 5 second'
     * <p>
     * curl 'http://localhost:9999/realtime/create' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"project": "projectId", "name": "Events by collection", "aggregation": "COUNT"}'
     *
     * @param report real-time report
     * @return a future that contains the operation status
     */
    @JsonRequest
    @ApiOperation(value = "Create report", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public CompletableFuture<SuccessMessage> createTable(@Named("project") RequestContext context, @BodyParam RealTimeReport report) {
        return realtimeService.create(context.project, report).thenApply(error -> {
            if (error == null) {
                return SuccessMessage.success();
            }

            return SuccessMessage.success(error.message);
        });
    }

    @JsonRequest
    @ApiOperation(value = "List queries", authorizations = @Authorization(value = "read_key"))

    @Path("/list")
    public List<RealTimeReport> listTables(@Named("project") RequestContext context) {
        return realtimeService.list(context.project);
    }

    @JsonRequest
    @POST
    @ApiOperation(value = "Get report", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Report does not exist.")})
    @Path("/get")
    public CompletableFuture<RealTimeQueryResult> queryTable(
            @Named("project") RequestContext context,
            @ApiParam("table_name") String tableName,
            @ApiParam(value = "filter", required = false) String filter,
            @ApiParam("measure") RealTimeReport.Measure measure,
            @ApiParam(value = "dimensions", required = false) List<String> dimensions,
            @ApiParam(value = "aggregate", required = false) Boolean aggregate,
            @ApiParam(value = "date_start", required = false) Instant dateStart,
            @ApiParam(value = "date_end", required = false) Instant dateEnd) {
        return realtimeService.query(context.project, tableName, filter, measure, dimensions, aggregate, dateStart, dateEnd);
    }

    @JsonRequest
    @ApiOperation(value = "Delete report", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    public CompletableFuture<SuccessMessage> deleteTable(@Named("project") RequestContext context,
                                                         @ApiParam("table_name") String tableName) {
        return realtimeService.delete(context.project, tableName).thenApply(result -> {
            if (result == null) {
                return SuccessMessage.success();
            } else {
                throw new RakamException(result.message, BAD_REQUEST);
            }
        });
    }
}
