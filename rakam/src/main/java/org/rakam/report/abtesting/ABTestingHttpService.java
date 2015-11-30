package org.rakam.report.abtesting;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.ui.JDBCReportMetadata;
import org.rakam.ui.Report;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.Path;

@Path("/ab-testing")
@Api(value = "/ab-testing", description = "A/B Testing module", tags = {"ab-testing"})
public class ABTestingHttpService extends HttpService {

    private final JDBCReportMetadata metadata;

    @Inject
    public ABTestingHttpService(JDBCReportMetadata metadata) {
        this.metadata = metadata;
    }

    @JsonRequest
    @Path("/list")
    public Object list(@ApiParam(name="project", value = "Project id", required = true) String project) {
        return metadata.getReports(project);
    }

    @JsonRequest
    @Path("/create")
    public JsonResponse create(@ParamBody Report report) {
        metadata.save(report);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/delete")
    public JsonResponse delete(@ApiParam(name="project", value = "Project id", required = true) String project,
                               @ApiParam(name="name", value = "Project name", required = true) String name) {
        metadata.delete(project, name);

        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/get")
    public Report get(@ApiParam(name="project", value = "Project id", required = true) String project,
                      @ApiParam(name="slug", value = "Report name", required = true) String slug) {
        return metadata.get(project, slug);
    }

    @JsonRequest
    @Path("/update")
    public Report update(@ParamBody Report report) {
        return metadata.update(report);
    }
}
