package org.rakam.ui.report;

import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.ui.JDBCReportMetadata;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.util.JsonHelper.encode;


@Path("/report")
@Api(value = "/report", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
@IgnoreApi
public class ReportHttpService extends HttpService {

    private final JDBCReportMetadata metadata;
    private final EncryptionConfig encryptionConfig;

    @Inject
    public ReportHttpService(JDBCReportMetadata metadata, EncryptionConfig encryptionConfig) {
        this.metadata = metadata;
        this.encryptionConfig = encryptionConfig;
    }

    @JsonRequest
    @ApiOperation(value = "List Reports", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    public Object list(@ApiParam(name = "project", value = "Project id", required = true) String project) {
        return metadata.getReports(project);
    }

    @ApiOperation(value = "Create Report", authorizations = @Authorization(value = "read_key"),
            response = JsonResponse.class, request = Report.class)
    @Path("/create")
    @POST
    public void create(RakamHttpRequest request) {
        request.bodyHandler(body -> {
            Report report = JsonHelper.read(body, Report.class);

            Optional<Integer> user = request.cookies().stream().filter(a -> a.name().equals("session")).findFirst()
                    .map(cookie -> WebUserHttpService.extractUserFromCookie(cookie.value(), encryptionConfig.getSecretKey()));

            if (!user.isPresent()) {
                request.response(encode(JsonResponse.error("Unauthorized")), UNAUTHORIZED).end();
            } else {
                metadata.save(user.get(), report);
                request.response(encode(JsonResponse.success()), OK).end();
            }
        });
    }

    @JsonRequest
    @ApiOperation(value = "Delete Report", authorizations = @Authorization(value = "read_key"))
    @Path("/delete")
    public JsonResponse delete(@ApiParam(name = "project", value = "Project id", required = true) String project,
                               @ApiParam(name = "slug", value = "Slug", required = true) String slug) {
        metadata.delete(project, slug);

        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public Report get(@ApiParam(name = "project", value = "Project id", required = true) String project,
                      @ApiParam(name = "slug", value = "Report name", required = true) String slug) {
        return metadata.get(project, slug);
    }

    @JsonRequest
    @ApiOperation(value = "Update report", authorizations = @Authorization(value = "read_key"))
    @Path("/update")
    public Report update(@ParamBody Report report) {
        return metadata.update(report);
    }
}
