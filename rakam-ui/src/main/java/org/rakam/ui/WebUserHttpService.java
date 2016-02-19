package org.rakam.ui;

import com.google.inject.Inject;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.util.CharsetUtil;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.IgnorePermissionCheck;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static io.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.cookie.ServerCookieEncoder.STRICT;
import static org.rakam.util.JsonResponse.error;

@Path("/ui/user")
public class WebUserHttpService extends HttpService {

    private final WebUserService service;

    @Inject
    public WebUserHttpService(WebUserService service) {
        this.service = service;
    }

    @JsonRequest
    @IgnorePermissionCheck
    @Path("/register")
    public Response register(@ApiParam(name = "email") String email,
                             @ApiParam(name = "name") String name,
                             @ApiParam(name = "password") String password) {
        // TODO: implement captcha https://github.com/VividCortex/angular-recaptcha https://developers.google.com/recaptcha/docs/verify
        // keep a counter for ip in local nodes and use stickiness feature of load balancer
        final WebUser user = service.createUser(email, name, password);
        return getLoginResponseForUser(user);
    }

    @JsonRequest
    @IgnorePermissionCheck
    @Path("/create-project")
    public UserApiKey createProject(@ApiParam(name = "name") String name,
                                    @CookieParam(name = "session") String session) {
        final UserApiKey newApiKey = service.createProject(extractUserFromCookie(session), name);
        return newApiKey;
    }

    @JsonRequest
    @IgnorePermissionCheck
    @Path("/create-api-keys")
    public Metastore.ProjectApiKeys createApiKeys(@ApiParam(name = "project") String project, @CookieParam(name = "session") String session) {
        return service.createApiKeys(extractUserFromCookie(session), project);
    }

    @JsonRequest
    @IgnorePermissionCheck
    @Path("/revoke-api-keys")
    public JsonResponse revokeApiKeys(@ApiParam(name = "project") String project, @ApiParam(name = "id") int id, @CookieParam(name = "session") String session) {
        service.revokeApiKeys(extractUserFromCookie(session), project, id);
        return JsonResponse.success();
    }

    @GET
    @IgnorePermissionCheck
    @Path("/me")
    public void me(RakamHttpRequest request) {
        String cookie = request.headers().get(COOKIE);

        List<String> jsonpParam = request.params().get("jsonp");
        Optional<String> jsonp = jsonpParam == null ? Optional.empty() : jsonpParam.stream().findAny();

        if (cookie != null) {
            Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookie);
            Optional<Cookie> session = cookies.stream().filter(c -> c.name().equals("session")).findAny();

            if (jsonp.isPresent() && !jsonp.get().matches("^[A-Za-z]+$")) {
                throw new RakamException(BAD_REQUEST);
            }

            if (session.isPresent()) {
                Integer id = null;
                try {
                    id = extractUserFromCookie(session.get().value());
                } catch (Exception e) {
                    request.response(unauthorized(jsonp)).end();
                }

                if (id != null) {
                    final Optional<WebUser> user = service.getUser(id);

                    if (user.isPresent()) {
                        String encode = JsonHelper.encode(user.get());
                        if (jsonp.isPresent()) {
                            encode = jsonp.get() + "(" + encode + ")";
                        }
                        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
                                wrappedBuffer(encode.getBytes(CharsetUtil.UTF_8)));
                        response.headers().set(CONTENT_TYPE, "application/json; charset=utf-8");
                        request.response(response).end();
                        return;
                    }
                }
            }
        }

        request.response(unauthorized(jsonp)).end();
    }

    private FullHttpResponse unauthorized(Optional<String> jsonp) {
        String encode = JsonHelper.encode(error(UNAUTHORIZED.reasonPhrase()));
        if (jsonp.isPresent()) {
            encode = jsonp.get() + "(" + encode + ")";
        }
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
                wrappedBuffer(encode.getBytes(CharsetUtil.UTF_8)));

        DefaultCookie cookie = new DefaultCookie("session", "");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(0);
        response.headers().add(SET_COOKIE, STRICT.encode(cookie));
        response.headers().set(CONTENT_TYPE, "application/json; charset=utf-8");
        return response;
    }

    @JsonRequest
    @IgnorePermissionCheck
    @Path("/login")
    public Response<WebUser> login(@ApiParam(name = "email") String email,
                                   @ApiParam(name = "password") String password) {
        final Optional<WebUser> user = service.login(email, password);

        if (user.isPresent()) {
            return getLoginResponseForUser(user.get());
        }

        throw new RakamException("Account couldn't found.", HttpResponseStatus.NOT_FOUND);
    }

    private Response getLoginResponseForUser(WebUser user) {
        final long expiringTimestamp = Instant.now().plus(7, ChronoUnit.DAYS).getEpochSecond();
        final StringBuilder cookieData = new StringBuilder()
                .append(expiringTimestamp).append("|")
                .append(user.id);

        final String secureKey = CryptUtil.encryptWithHMacSHA1(cookieData.toString(), "secureKey");
        cookieData.append("|").append(secureKey);

        return Response.ok(user).addCookie("session", cookieData.toString(),
                null, true, Duration.ofDays(30).getSeconds(), "/", null);
    }

    private static int extractUserFromCookie(String session) {
        if (session == null) {
            throw new RakamException(UNAUTHORIZED);
        }
        final String[] split = session.split("\\|");
        if (split.length != 3) {
            throw new RakamException(UNAUTHORIZED);
        }

        final long expiringTimestamp = Long.parseLong(split[0]);
        final int id = Integer.parseInt(split[1]);
        final String hash = split[2];

        final StringBuilder cookieData = new StringBuilder()
                .append(expiringTimestamp).append("|")
                .append(id);
        if (!CryptUtil.encryptWithHMacSHA1(cookieData.toString(), "secureKey").equals(hash)) {
            throw new RakamException(UNAUTHORIZED);
        }

        return id;
    }
}
