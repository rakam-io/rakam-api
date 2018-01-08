package org.rakam.analysis.abtesting;

import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.RequestContext;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

@Path("/ab-testing")
@Api(value = "/ab-testing", nickname = "abTesting", description = "A/B Testing module", tags = "ab-testing")
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
    public List<ABTestingReport> list(@Named("project") RequestContext context) {
        return metadata.getReports(context.project);
    }

    @JsonRequest
    @ApiOperation(value = "Create test", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public SuccessMessage create(@Named("project") RequestContext context, @BodyParam ABTestingReport report) {
        metadata.save(context.project, report);
        return SuccessMessage.success();
    }

    @Path("/data")
    @GET
    @IgnoreApi
    public void data(RakamHttpRequest request) {
        Map<String, List<String>> params = request.params();
        List<String> api_key = params.get("read_key");
        if (api_key == null || api_key.isEmpty()) {
            request.response("\"read_key is missing\"", BAD_REQUEST).end();
            return;
        }

        // since this endpoint is created for clients to read the ab-testing rule,
        // the permission is WRITE_KEY
        String project = apiKeyService.getProjectOfApiKey(api_key.get(0), ApiKeyService.AccessKeyType.WRITE_KEY);

        request.response(JsonHelper.encodeAsBytes(metadata.getReports(project)))
                .end();
    }

    @JsonRequest
    @ApiOperation(value = "Delete report", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    public SuccessMessage delete(@Named("project") RequestContext context,
                                 @ApiParam("id") int id) {
        metadata.delete(context.project, id);
        return SuccessMessage.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public ABTestingReport get(@Named("project") RequestContext context, @ApiParam("id") int id) {
        return metadata.get(context.project, id);
    }

    @JsonRequest
    @ApiOperation(value = "Update report", authorizations = @Authorization(value = "master_key"))
    @Path("/update")
    public ABTestingReport update(@Named("project") RequestContext context, @BodyParam ABTestingReport report) {
        return metadata.update(context.project, report);
    }
}
