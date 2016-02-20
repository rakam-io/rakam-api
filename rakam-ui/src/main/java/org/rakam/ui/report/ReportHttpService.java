package org.rakam.ui.report;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.ui.JDBCReportMetadata;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.Path;


@Path("/report")
@Api(value = "/report", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
@IgnoreApi
public class ReportHttpService extends HttpService {

    private final JDBCReportMetadata metadata;

    @Inject
    public ReportHttpService(JDBCReportMetadata metadata) {
        this.metadata = metadata;
    }

    @JsonRequest
    @ApiOperation(value = "List Reports", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    public Object list(@ApiParam(name="project", value = "Project id", required = true) String project) {
        return metadata.getReports(project);
    }

    @JsonRequest
    @ApiOperation(value = "Create Report", authorizations = @Authorization(value = "read_key"))
    @Path("/create")
    public JsonResponse create(@ParamBody Report report) {
        metadata.save(report);
        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Delete Report", authorizations = @Authorization(value = "read_key"))
    @Path("/delete")
    public JsonResponse delete(@ApiParam(name="project", value = "Project id", required = true) String project,
                               @ApiParam(name="slug", value = "Slug", required = true) String slug) {
        metadata.delete(project, slug);

        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public Report get(@ApiParam(name="project", value = "Project id", required = true) String project,
                      @ApiParam(name="slug", value = "Report name", required = true) String slug) {
        return metadata.get(project, slug);
    }

    @JsonRequest
    @ApiOperation(value = "Update report", authorizations = @Authorization(value = "read_key"))
    @Path("/update")
    public Report update(@ParamBody Report report) {
        return metadata.update(report);
    }
}
