package org.rakam.analysis.abtesting;

import org.rakam.analysis.ApiKeyService;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@Path("/ab-testing")
@Api(value = "/ab-testing", nickname = "abTesting", description = "A/B Testing module", tags = {"ab-testing"})
public class ABTestingHttpService extends HttpService {

    private final ABTestingMetastore metadata;
    private final ApiKeyService apiKeyService;

    @Inject
    public ABTestingHttpService(ApiKeyService apiKeyService, ABTestingMetastore metadata) {
        this.metadata = metadata;
        this.apiKeyService = apiKeyService;
    }

    @GET
    @ApiOperation(value = "List reports", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    public List<ABTestingReport> list(@Named("project") String project) {
        return metadata.getReports(project);
    }

    @JsonRequest
    @ApiOperation(value = "Create test", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public JsonResponse create(@Named("project") String project, @BodyParam ABTestingReport report) {
        metadata.save(project, report);
        return JsonResponse.success();
    }

    @Path("/list")
    @GET
    @IgnoreApi
    @IgnorePermissionCheck
    public void data(RakamHttpRequest request) {
        Map<String, List<String>> params = request.params();
        List<String> project = params.get("project");
        List<String> api_key = params.get("api_key");
        if(project == null || project.isEmpty()) {
            request.response("\"project is missing\"", BAD_REQUEST).end();
            return;
        }
        if(api_key == null || api_key.isEmpty()) {
            request.response("\"api_key is missing\"", BAD_REQUEST).end();
            return;
        }

        // since this endpoint is created for clients to read the ab-testing rule,
        // the permission is WRITE_KEY
        if(!apiKeyService.checkPermission(project.get(0), ApiKeyService.AccessKeyType.WRITE_KEY, api_key.get(0))) {
            request.response("\"unauthorized\"", UNAUTHORIZED).end();
            return;
        }

        request.response(JsonHelper.encodeAsBytes(metadata.getReports(project.get(0))))
                .end();
    }

    @JsonRequest
    @ApiOperation(value = "Delete report", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    public JsonResponse delete(@Named("project") String project,
                               @ApiParam("id") int id) {
        metadata.delete(project, id);
        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public ABTestingReport get(@Named("project") String project, @ApiParam("id") int id) {
        return metadata.get(project, id);
    }

    @JsonRequest
    @ApiOperation(value = "Update report", authorizations = @Authorization(value = "master_key"))
    @Path("/update")
    public ABTestingReport update(@Named("project") String project, @BodyParam ABTestingReport report) {
        return metadata.update(project, report);
    }
}
