package org.rakam.ui.report;

import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.RakamUIModule;
import org.rakam.ui.ReportMetadata;
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.ui.user.WebUserHttpService.extractUserFromCookie;
import static org.rakam.util.JsonHelper.encode;

@Path("/ui/report")
@Api(value = "/report", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
@IgnoreApi
@RakamUIModule.UIService
public class ReportHttpService
        extends HttpService
{
    private final ReportMetadata metadata;
    private final EncryptionConfig encryptionConfig;

    @Inject
    public ReportHttpService(ReportMetadata metadata, EncryptionConfig encryptionConfig)
    {
        this.metadata = metadata;
        this.encryptionConfig = encryptionConfig;
    }

    @JsonRequest
    @ApiOperation(value = "List Reports", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    public Object list(@HeaderParam("project") int project,
            @CookieParam("session") String session)
    {
        int userId = extractUserFromCookie(session, encryptionConfig.getSecretKey());
        return metadata.getReports(userId, project);
    }

    @ApiOperation(value = "Create Report", authorizations = @Authorization(value = "read_key"))
    @Path("/create")
    @POST
    public SuccessMessage create(@HeaderParam("project") int project, @CookieParam("session") String session, @BodyParam Report report)
    {

        Optional<Integer> user = Optional.ofNullable(session)
                .map(cookie -> extractUserFromCookie(cookie, encryptionConfig.getSecretKey()));

        if (!user.isPresent()) {
            throw new RakamException(UNAUTHORIZED);
        }
        else {
            metadata.save(user.get(), project, report);
            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Delete Report", authorizations = @Authorization(value = "read_key"))
    @Path("/delete")
    public SuccessMessage delete(@HeaderParam("project") int project,
            @ApiParam(value = "slug", description = "Slug") String slug,
            @CookieParam("session") String session)
    {
        metadata.delete(extractUserFromCookie(session, encryptionConfig.getSecretKey()),
                project, slug);

        return SuccessMessage.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public Report get(@HeaderParam("project") int project,
            @ApiParam(value = "slug", description = "Report name") String slug,
            @ApiParam(value = "user_id", required = false, description = "Report user id") Integer userId,
            @CookieParam("session") String session)
    {
        return metadata.get(extractUserFromCookie(session, encryptionConfig.getSecretKey()), userId, project, slug);
    }

    @ApiOperation(value = "Update report", authorizations = @Authorization(value = "read_key"))
    @POST
    @Path("/update")
    public void update(@HeaderParam("project") int project, RakamHttpRequest request)
    {
        request.bodyHandler(body -> {
            Report report = JsonHelper.read(body, Report.class);

            Optional<Integer> user = request.cookies().stream().filter(a -> a.name().equals("session")).findFirst()
                    .map(cookie -> extractUserFromCookie(cookie.value(), encryptionConfig.getSecretKey()));

            if (!user.isPresent()) {
                throw new RakamException(UNAUTHORIZED);
            }
            else {
                metadata.update(user.get(), project, report);
                request.response(encode(SuccessMessage.success()), OK).end();
            }
        });
    }
}
