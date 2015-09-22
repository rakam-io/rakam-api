package org.rakam.ui;

import javax.inject.Inject;
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

import javax.ws.rs.Path;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/04/15 22:43.
 */
@Path("/report")
@Api(value = "/report", description = "Report analyzer module", tags = "report")
public class ReportHttpService extends HttpService {

    private final JDBCReportMetadata metadata;

    @Inject
    public ReportHttpService(JDBCReportMetadata metadata) {
        this.metadata = metadata;
    }

    @JsonRequest
    @ApiOperation(value = "List reports",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/list")
    public Object list(@ApiParam(name="project", value = "Project id", required = true) String project) {
        return metadata.getReports(project);
    }

    @JsonRequest
    @ApiOperation(value = "Create new report",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/create")
    public JsonResponse create(@ParamBody Report report) {
        metadata.save(report);
        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Delete report", notes = "Creates report and cached data.",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/delete")
    public JsonResponse delete(@ApiParam(name="project", value = "Project id", required = true) String project,
                               @ApiParam(name="name", value = "Project name", required = true) String name) {
        metadata.delete(project, name);

        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get report", notes = "Returns report that has the specified name",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.") })
    @Path("/get")
    public Report get(@ApiParam(name="project", value = "Project id", required = true) String project,
                      @ApiParam(name="slug", value = "Report name", required = true) String slug) {
        return metadata.get(project, slug);
    }
}
