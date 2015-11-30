package org.rakam.report.abtesting;

import org.rakam.plugin.IgnorePermissionCheck;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;

@Path("/ab-testing")
@Api(value = "/ab-testing", description = "A/B Testing module", tags = {"ab-testing"})
public class ABTestingHttpService extends HttpService {

    private final ABTestingMetastore metadata;

    @Inject
    public ABTestingHttpService(ABTestingMetastore metadata) {
        this.metadata = metadata;
    }

    @JsonRequest
    @ApiOperation(value = "List reports")
    @Path("/list")
    public Object list(@ApiParam(name="project", value = "Project id", required = true) String project) {
        return metadata.getReports(project);
    }

    @JsonRequest
    @ApiOperation(value = "Create test")
    @Path("/create")
    public JsonResponse create(@ParamBody ABTestingReport report) {
        metadata.save(report);
        return JsonResponse.success();
    }

    @Path("/data")
    @GET
    @IgnoreApi
    @IgnorePermissionCheck
    public void data(RakamHttpRequest request) {
        Map<String, List<String>> params = request.params();
        params.get("project");
        params.get("api_key");
        params.get("options");
        request.end();
    }

    @JsonRequest
    @ApiOperation(value = "Delete report")
    @Path("/delete")
    public JsonResponse delete(@ApiParam(name="project") String project,
                               @ApiParam(name="id") int id) {
        metadata.delete(project, id);

        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get report")
    @Path("/get")
    public ABTestingReport get(@ApiParam(name="project") String project,
                      @ApiParam(name="id") int id) {
        return metadata.get(project, id);
    }

    @JsonRequest
    @ApiOperation(value = "Update report")
    @Path("/update")
    public ABTestingReport update(@ParamBody ABTestingReport report) {
        return metadata.update(report);
    }
}
