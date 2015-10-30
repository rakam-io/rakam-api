package org.rakam.ui;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.Response;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.WebUser.UserApiKey;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@Path("/ui/user")
public class WebUserHttpService extends HttpService {

    private final WebUserService service;

    @Inject
    public WebUserHttpService(WebUserService service) {
        this.service = service;
    }

    @JsonRequest
    @Path("/register")
    public JsonResponse register(@ApiParam(name="email") String email,
                          @ApiParam(name="name") String name,
                          @ApiParam(name="password") String password) {
        // TODO: implement captcha https://github.com/VividCortex/angular-recaptcha https://developers.google.com/recaptcha/docs/verify
        // keep a counter for ip in local nodes and use stickiness feature of load balancer
        service.createUser(email, name, password);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/create-project")
    public UserApiKey createProject(@ApiParam(name = "name") String name,
                                            @CookieParam(name = "session") String session) {
        final UserApiKey newApiKey = service.createProject(extractUserFromCookie(session), name);
        return newApiKey;
    }

    @JsonRequest
    @GET
    @Path("/me")
    public Response<WebUser> me(@CookieParam(name="session", required = false) String session) {
        final int id;
        try {
            id = extractUserFromCookie(session);
        } catch (Exception e) {
            return Response.value(JsonHelper.encode(JsonResponse.error(UNAUTHORIZED.reasonPhrase())), UNAUTHORIZED)
                    .addCookie("session", "", null, true, 0L, null, null);
        }

        final Optional<WebUser> user = service.getUser(id);

        if(!user.isPresent()) {
            return Response.value(JsonHelper.encode(JsonResponse.error(UNAUTHORIZED.reasonPhrase())), UNAUTHORIZED)
                    .addCookie("session", "", null, true, 0L, null, null);
        }

        return Response.ok(user.get());
    }

    @JsonRequest
    @Path("/login")
    public Response<WebUser> login(@ApiParam(name="email") String email,
                               @ApiParam(name="password") String password) {
        final Optional<WebUser> user = service.login(email, password);

        if(user.isPresent()) {
            final long expiringTimestamp = Instant.now().plus(7, ChronoUnit.DAYS).getEpochSecond();
            final StringBuilder cookieData = new StringBuilder()
                    .append(expiringTimestamp).append("|")
                    .append(user.get().id);

            final String secureKey = CryptUtil.encryptWithHMacSHA1(cookieData.toString(), "secureKey");
            cookieData.append("|").append(secureKey);

            return Response.ok(user.get()).addCookie("session", cookieData.toString(),
                    null, true, Duration.ofDays(30).getSeconds(), "/", null);
        }

        throw new RakamException("Account couldn't found.", HttpResponseStatus.NOT_FOUND);
    }

    private static int extractUserFromCookie(String session) {
        if(session == null) {
            throw new RakamException(UNAUTHORIZED);
        }
        final String[] split = session.split("\\|");
        if(split.length != 3) {
            throw new RakamException(UNAUTHORIZED);
        }

        final long expiringTimestamp = Long.parseLong(split[0]);
        final int id = Integer.parseInt(split[1]);
        final String hash = split[2];

        final StringBuilder cookieData = new StringBuilder()
                .append(expiringTimestamp).append("|")
                .append(id);
        if(!CryptUtil.encryptWithHMacSHA1(cookieData.toString(), "secureKey").equals(hash)) {
            throw new RakamException(UNAUTHORIZED);
        }

        return id;
    }
}
