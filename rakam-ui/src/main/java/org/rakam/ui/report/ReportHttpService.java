package org.rakam.ui.report;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.JDBCReportMetadata;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;
import org.rakam.util.SentryUtil;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.ui.user.WebUserHttpService.extractUserFromCookie;
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
    public Object list(@javax.inject.Named("project") String project,
                       @CookieParam(name = "session") String session) {
        int userId = extractUserFromCookie(session, encryptionConfig.getSecretKey());
        return metadata.getReports(userId, project);
    }

    @ApiOperation(value = "Create Report", authorizations = @Authorization(value = "read_key"))
    @Path("/create")
    @POST
    public void create(@Named("project") String project, RakamHttpRequest request) {
        request.bodyHandler(body -> {
            Report report = JsonHelper.read(body, Report.class);

            Optional<Integer> user = request.cookies().stream().filter(a -> a.name().equals("session")).findFirst()
                    .map(cookie -> extractUserFromCookie(cookie.value(), encryptionConfig.getSecretKey()));

            JsonResponse response;
            HttpResponseStatus status;
            if (!user.isPresent()) {
                response = JsonResponse.error("Unauthorized");
                status = UNAUTHORIZED;
            } else {
                try {
                    metadata.save(user.get(), project, report);

                    response = JsonResponse.success();
                    status = OK;
                } catch (RakamException e) {
                    SentryUtil.logException(request, e);
                    response = JsonResponse.error(e.getMessage());
                    status = e.getStatusCode();
                }
            }

            FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                    Unpooled.wrappedBuffer(encode(response).getBytes(CharsetUtil.UTF_8)));
            resp.headers().set("Content-Type", "application/json");
            request.response(resp).end();
        });
    }

    @JsonRequest
    @ApiOperation(value = "Delete Report", authorizations = @Authorization(value = "read_key"))
    @Path("/delete")
    public JsonResponse delete(@javax.inject.Named("project") String project,
                               @ApiParam(value="slug", description = "Slug") String slug,
                               @CookieParam(name = "session") String session) {
        metadata.delete(extractUserFromCookie(session, encryptionConfig.getSecretKey()),
                project, slug);

        return JsonResponse.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public Report get(@javax.inject.Named("project") String project,
                      @ApiParam(value="slug", description = "Report name") String slug,
                      @ApiParam(value="user_id", required = false, description = "Report user id") Integer userId,
                      @CookieParam(name = "session") String session) {
        return metadata.get(extractUserFromCookie(session, encryptionConfig.getSecretKey()), userId, project, slug);
    }

    @ApiOperation(value = "Update report", authorizations = @Authorization(value = "read_key"))
    @POST
    @Path("/update")
    public void update(@Named("project") String project, RakamHttpRequest request) {
        request.bodyHandler(body -> {
            Report report = JsonHelper.read(body, Report.class);

            Optional<Integer> user = request.cookies().stream().filter(a -> a.name().equals("session")).findFirst()
                    .map(cookie -> extractUserFromCookie(cookie.value(), encryptionConfig.getSecretKey()));

            if (!user.isPresent()) {
                request.response(encode(JsonResponse.error("Unauthorized")), UNAUTHORIZED).end();
            } else {
                metadata.update(user.get(), project, report);
                request.response(encode(JsonResponse.success()), OK).end();
            }
        });
    }
}
