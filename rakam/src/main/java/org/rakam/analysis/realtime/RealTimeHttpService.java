package org.rakam.analysis.realtime;

import com.google.inject.Singleton;
import org.rakam.analysis.RealtimeService;
import org.rakam.analysis.RealtimeService.RealTimeQueryResult;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.realtime.RealTimeReport;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.SuccessMessage;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.rakam.util.ValidationUtil.checkTableColumn;

@Singleton
@Api(value = "/realtime", nickname = "realtime", description = "Realtime module", tags = "realtime")
@Path("/realtime")
public class RealTimeHttpService
        extends HttpService
{
    private final RealtimeService realtimeService;

    @Inject
    public RealTimeHttpService(RealtimeService realtimeService)
    {
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
    public CompletableFuture<SuccessMessage> createTable(@Named("project") String project, @BodyParam RealTimeReport report)
    {
        return realtimeService.create(project, report);
    }

    @JsonRequest
    @ApiOperation(value = "List queries", authorizations = @Authorization(value = "read_key"))

    @Path("/list")
    public List<ContinuousQuery> listTables(@Named("project") String project)
    {
        return realtimeService.list(project);
    }

    @JsonRequest
    @POST
    @ApiOperation(value = "Get report", authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Report does not exist.")})
    @Path("/get")
    public CompletableFuture<RealTimeQueryResult> queryTable(@Named("project") String project,
            @ApiParam("table_name") String tableName,
            @ApiParam(value = "filter", required = false) String filter,
            @ApiParam("measure") RealTimeReport.Measure measure,
            @ApiParam(value = "dimensions", required = false) List<String> dimensions,
            @ApiParam(value = "aggregate", required = false) Boolean aggregate,
            @ApiParam(value = "date_start", required = false) Instant dateStart,
            @ApiParam(value = "date_end", required = false) Instant dateEnd)
    {
        return realtimeService.query(project, tableName, filter, measure, dimensions, aggregate, dateStart, dateEnd);
    }

    @JsonRequest
    @ApiOperation(value = "Delete report", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    public CompletableFuture<SuccessMessage> deleteTable(@Named("project") String project,
            @ApiParam("table_name") String tableName)
    {
        // TODO: Check if it's a real-time report.
        return realtimeService.delete(project, tableName).thenApply(result -> {
            if (result) {
                return SuccessMessage.success();
            }
            else {
                throw new RakamException("Couldn't delete report. It doesn't exist", BAD_REQUEST);
            }
        });
    }
}
