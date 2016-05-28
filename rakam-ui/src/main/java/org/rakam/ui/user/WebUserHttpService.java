package org.rakam.ui.user;

import com.google.inject.Inject;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.util.CharsetUtil;
import org.rakam.analysis.ApiKeyService;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.Response;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.user.WebUser.UserApiKey;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.cookie.ServerCookieEncoder.STRICT;
import static org.rakam.util.JsonResponse.error;

@Path("/ui/user")
@IgnoreApi
public class WebUserHttpService extends HttpService {

    private final WebUserService service;
    private final EncryptionConfig encryptionConfig;

    @Inject
    public WebUserHttpService(WebUserService service, EncryptionConfig encryptionConfig) {
        this.service = service;
        this.encryptionConfig = encryptionConfig;
    }

    @JsonRequest

    @Path("/register")
    public Response register(@ApiParam("email") String email,
                             @ApiParam("name") String name,
                             @ApiParam("password") String password) {
        // TODO: implement captcha https://github.com/VividCortex/angular-recaptcha https://developers.google.com/recaptcha/docs/verify
        // keep a counter for ip in local nodes and use stickiness feature of load balancer
        final WebUser user = service.createUser(email, name, password);
        return getLoginResponseForUser(user);
    }

    @JsonRequest

    @Path("/update/password")
    public JsonResponse update(@ApiParam("oldPassword") String oldPassword,
                               @ApiParam("newPassword") String newPassword,
                               @CookieParam("session") String session) {
        service.updateUserPassword(extractUserFromCookie(session, encryptionConfig.getSecretKey()), oldPassword, newPassword);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/update/info")
    public JsonResponse update(@ApiParam("name") String name, @CookieParam("session") String session) {
        service.updateUserInfo(extractUserFromCookie(session, encryptionConfig.getSecretKey()), name);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/create-project")
    public UserApiKey createProject(@ApiParam("name") String name,
                                    @ApiParam("api_url") String apiUrl,
                                    @CookieParam("session") String session) {
        int user = extractUserFromCookie(session, encryptionConfig.getSecretKey());
        return service.createProject(user, apiUrl, name);
    }

    @JsonRequest
    @Path("/register-project")
    public UserApiKey registerProject(@ApiParam("name") String name,
                                      @ApiParam("api_url") String apiUrl,
                                      @ApiParam("read_key") String readKey,
                                      @ApiParam("write_key") String writeKey,
                                      @ApiParam("master_key") String masterKey,
                                      @CookieParam("session") String session) {
        int user = extractUserFromCookie(session, encryptionConfig.getSecretKey());
        return service.registerProject(user, apiUrl, name, readKey, writeKey, masterKey);
    }


    @JsonRequest

    @Path("/delete-project")
    public JsonResponse deleteProject(@ApiParam("name") String name,
                                      @ApiParam("api_url") String apiUrl,
                                      @CookieParam("session") String session) {
        int user = extractUserFromCookie(session, encryptionConfig.getSecretKey());
        service.deleteProject(user, apiUrl, name);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/create-api-keys")
    public ApiKeyService.ProjectApiKeys createApiKeys(@ApiParam("project") String project, @ApiParam("api_url") String apiUrl, @CookieParam("session") String session) {
        return service.createApiKeys(extractUserFromCookie(session, encryptionConfig.getSecretKey()), project, apiUrl);
    }

    @JsonRequest
    @Path("/revoke-api-keys")
    public JsonResponse revokeApiKeys(@ApiParam("api_url") String apiUrl, @ApiParam("api_key") String key, @CookieParam("session") String session) {
        service.revokeApiKeys(extractUserFromCookie(session, encryptionConfig.getSecretKey()), apiUrl, key);
        return JsonResponse.success();
    }


    @JsonRequest
    @ApiOperation(value = "List users who can access to the project", authorizations = @Authorization(value = "master_key"))
    @Path("/user-access")
    public List<WebUserService.UserAccess> getUserAccess(@CookieParam("session") String session,
                                                         @HeaderParam("project") int project) {
        return service.getUserAccessForAllProjects(extractUserFromCookie(session, encryptionConfig.getSecretKey()), project);
    }

    @JsonRequest
    @ApiOperation(value = "Recover my password", authorizations = @Authorization(value = "master_key"))

    @Path("/prepare-recover-password")
    public JsonResponse prepareRecoverPassword(@ApiParam("email") String email) {
        service.prepareRecoverPassword(email);
        return JsonResponse.success();
    }

    @ApiOperation(value = "Recover my password", authorizations = @Authorization(value = "master_key"))
    @JsonRequest

    @Path("/perform-recover-password")
    public JsonResponse performRecoverPassword(@ApiParam("key") String key,
                                               @ApiParam("hash") String hash,
                                               @ApiParam("password") String password) {
        service.performRecoverPassword(key, hash, password);
        return JsonResponse.success();
    }

    @JsonRequest

    @ApiOperation(value = "Revoke User Access", authorizations = @Authorization(value = "master_key"))
    @Path("/revoke-user-access")
    public JsonResponse revokeUserAccess(@CookieParam("session") String session,
                                         @ApiParam("project") String project,
                                         @ApiParam("api_url") String api_url,
                                         @ApiParam("email") String email) {
        Optional<WebUser> user = service.getUser(extractUserFromCookie(session, encryptionConfig.getSecretKey()));
        if (!user.isPresent()) {
            throw new RakamException(BAD_REQUEST);
        }

        boolean hasPermission = user.get().projects.stream().anyMatch(e -> e.name.equals(project) &&
                Objects.equals(e.apiUrl, api_url) &&
                e.apiKeys.stream().anyMatch(a -> a.masterKey() != null));

        if (!hasPermission) {
            throw new RakamException(UNAUTHORIZED);
        }

        service.revokeUserAccess(project, api_url, email);
        return JsonResponse.success();
    }

    @JsonRequest

    @Path("/give-user-access")
    public JsonResponse giveUserAccess(@CookieParam("session") String session,
                                       @ApiParam("project") String project,
                                       @ApiParam("api_url") String api_url,
                                       @ApiParam("email") String email,
                                       @ApiParam(value = "scope_expression", required = false) String scopeExpression,
                                       @ApiParam("has_read_permission") boolean has_read_permission,
                                       @ApiParam("has_write_permission") boolean has_write_permission,
                                       @ApiParam("is_admin") boolean isAdmin) {
        Optional<WebUser> user = service.getUser(extractUserFromCookie(session, encryptionConfig.getSecretKey()));
        if (!user.isPresent()) {
            throw new RakamException(BAD_REQUEST);
        }

        Optional<String> masterKey = user.get().projects.stream().filter(e -> e.name.equals(project) &&
                Objects.equals(e.apiUrl, api_url) &&
                e.apiKeys.stream().anyMatch(a -> a.masterKey() != null)).findAny()
                .flatMap(e -> e.apiKeys.stream().filter(a -> a.masterKey() != null).findFirst().map(k -> k.masterKey()));

        masterKey.orElseThrow(() -> new RakamException(UNAUTHORIZED));

        service.giveAccessToUser(project, api_url, masterKey.get(), email, scopeExpression, has_read_permission, has_write_permission, isAdmin);
        return JsonResponse.success();
    }

    @GET

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
                    id = extractUserFromCookie(session.get().value(), encryptionConfig.getSecretKey());
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

    @Path("/login")
    public Response<WebUser> login(@ApiParam("email") String email,
                                   @ApiParam("password") String password) {
        final Optional<WebUser> user = service.login(email, password);

        if (user.isPresent()) {
            return getLoginResponseForUser(user.get());
        }

        throw new RakamException("Account couldn't found.", HttpResponseStatus.NOT_FOUND);
    }


    @GET
    @Path("/logout")
    public Response<JsonResponse> logout() {
        return Response.ok(JsonResponse.success()).addCookie("session", "", null, true, -1L, "/", null);
    }

    private Response getLoginResponseForUser(WebUser user) {
        final long expiringTimestamp = Instant.now().plus(7, ChronoUnit.DAYS).getEpochSecond();
        final StringBuilder cookieData = new StringBuilder()
                .append(expiringTimestamp).append("|")
                .append(user.id);

        final String secureKey = CryptUtil.encryptWithHMacSHA1(cookieData.toString(), encryptionConfig.getSecretKey());
        cookieData.append('|').append(secureKey);

        return Response.ok(user).addCookie("session", cookieData.toString(),
                null, true, Duration.ofDays(30).getSeconds(), "/", null);
    }

    public static int extractUserFromCookie(String session, String key) {
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
        if (!CryptUtil.encryptWithHMacSHA1(cookieData.toString(), key).equals(hash)) {
            throw new RakamException(UNAUTHORIZED);
        }

        return id;
    }
}
