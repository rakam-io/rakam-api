package org.rakam.ui.user;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Userinfoplus;
import com.google.inject.Inject;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.DefaultCookie;
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
import org.rakam.ui.RakamUIConfig;
import org.rakam.ui.user.WebUser.UserApiKey;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.cookie.ServerCookieEncoder.STRICT;

@Path("/ui/user")
@IgnoreApi
public class WebUserHttpService
        extends HttpService
{

    private final WebUserService service;
    private final EncryptionConfig encryptionConfig;
    private final RakamUIConfig config;

    @Inject
    public WebUserHttpService(WebUserService service, RakamUIConfig config, EncryptionConfig encryptionConfig)
    {
        this.service = service;
        this.encryptionConfig = encryptionConfig;
        this.config = config;
    }

    @JsonRequest

    @Path("/register")
    public Response register(@ApiParam("email") String email,
            @ApiParam("name") String name,
            @ApiParam("password") String password)
    {
        // TODO: implement captcha https://github.com/VividCortex/angular-recaptcha https://developers.google.com/recaptcha/docs/verify
        // keep a counter for ip in local nodes and use stickiness feature of load balancer
        final WebUser user = service.createUser(email, name, password, null, null, null);
        return getLoginResponseForUser(user);
    }

    @JsonRequest

    @Path("/update/password")
    public SuccessMessage update(@ApiParam("oldPassword") String oldPassword,
            @ApiParam("newPassword") String newPassword,
            @CookieParam("session") String session)
    {
        service.updateUserPassword(extractUserFromCookie(session, encryptionConfig.getSecretKey()), oldPassword, newPassword);
        return SuccessMessage.success();
    }

    @JsonRequest
    @Path("/update/info")
    public SuccessMessage update(@ApiParam("name") String name, @CookieParam("session") String session)
    {
        service.updateUserInfo(extractUserFromCookie(session, encryptionConfig.getSecretKey()), name);
        return SuccessMessage.success();
    }

    @JsonRequest
    @Path("/create-project")
    public UserApiKey createProject(@ApiParam("name") String name,
            @ApiParam("api_url") String apiUrl,
            @CookieParam("session") String session)
    {
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
            @CookieParam("session") String session)
    {
        int user = extractUserFromCookie(session, encryptionConfig.getSecretKey());
        return service.registerProject(user, apiUrl, name, readKey, writeKey, masterKey);
    }

    @JsonRequest
    @Path("/delete-project")
    public SuccessMessage deleteProject(@ApiParam("name") String name,
            @ApiParam("api_url") String apiUrl,
            @CookieParam("session") String session)
    {
        int user = extractUserFromCookie(session, encryptionConfig.getSecretKey());
        service.deleteProject(user, apiUrl, name);
        return SuccessMessage.success();
    }

    @JsonRequest
    @Path("/save-api-keys")
    public ApiKeyService.ProjectApiKeys createApiKeys(@HeaderParam("project") int project,
            @CookieParam("session") String session,
            @ApiParam("read_key") String readKey,
            @ApiParam("write_key") String writeKey,
            @ApiParam("master_key") String masterKey)
    {
        service.saveApiKeys(extractUserFromCookie(session, encryptionConfig.getSecretKey()), project, readKey, writeKey, masterKey);
        return ApiKeyService.ProjectApiKeys.create(masterKey, readKey, writeKey);
    }

    @JsonRequest
    @Path("/revoke-api-keys")
    public SuccessMessage revokeApiKeys(@HeaderParam("project") int project, @ApiParam("master_key") String key, @CookieParam("session") String session)
    {
        service.revokeApiKeys(extractUserFromCookie(session, encryptionConfig.getSecretKey()), project, key);
        return SuccessMessage.success();
    }

    @JsonRequest
    @ApiOperation(value = "List users who can access to the project", authorizations = @Authorization(value = "master_key"))
    @Path("/user-access")
    public List<WebUserService.UserAccess> getUserAccess(@CookieParam("session") String session,
            @HeaderParam("project") int project)
    {
        return service.getUserAccessForProject(extractUserFromCookie(session, encryptionConfig.getSecretKey()), project);
    }

    @JsonRequest
    @ApiOperation(value = "Recover my password", authorizations = @Authorization(value = "master_key"))

    @Path("/prepare-recover-password")
    public SuccessMessage prepareRecoverPassword(@ApiParam("email") String email)
    {
        service.prepareRecoverPassword(email);
        return SuccessMessage.success();
    }

    @ApiOperation(value = "Recover my password", authorizations = @Authorization(value = "master_key"))
    @JsonRequest

    @Path("/perform-recover-password")
    public SuccessMessage performRecoverPassword(@ApiParam("key") String key,
            @ApiParam("hash") String hash,
            @ApiParam("password") String password)
    {
        service.performRecoverPassword(key, hash, password);
        return SuccessMessage.success();
    }

    @JsonRequest

    @ApiOperation(value = "Revoke User Access", authorizations = @Authorization(value = "master_key"))
    @Path("/revoke-user-access")
    public SuccessMessage revokeUserAccess(@CookieParam("session") String session,
            @HeaderParam("project") int project,
            @ApiParam("email") String email)
    {
        Optional<WebUser> user = service.getUser(extractUserFromCookie(session, encryptionConfig.getSecretKey()));
        if (!user.isPresent()) {
            throw new RakamException(BAD_REQUEST);
        }

        service.revokeUserAccess(user.get().id, project, email);
        return SuccessMessage.success();
    }

    @JsonRequest

    @Path("/give-user-access")
    public SuccessMessage giveUserAccess(@CookieParam("session") String session,
            @HeaderParam("project") int project,
            @ApiParam("email") String email,
            @ApiParam(value = "scope_expression", required = false) String scopeExpression,
            @ApiParam(value = "keys") ApiKeyService.ProjectApiKeys keys,
            @ApiParam(value = "read_permission") boolean readPermission,
            @ApiParam(value = "write_permission") boolean writePermission,
            @ApiParam(value = "master_permission") boolean masterPermission)
    {
        Optional<WebUser> user = service.getUser(extractUserFromCookie(session, encryptionConfig.getSecretKey()));
        if (!user.isPresent()) {
            throw new RakamException(BAD_REQUEST);
        }

        user.get().projects.stream().filter(e -> e.apiKeys.stream().anyMatch(a -> a.masterKey() != null));

        service.giveAccessToUser(project, user.get().id, email, keys, scopeExpression,
                readPermission, writePermission, masterPermission);
        return SuccessMessage.success();
    }

    @GET
    @JsonRequest
    @Path("/me")
    public void me(RakamHttpRequest request, @CookieParam("session") String session)
    {
        String cookie = request.headers().get(COOKIE);

        List<String> jsonpParam = request.params().get("jsonp");
        Optional<String> jsonp = jsonpParam == null ? Optional.empty() : jsonpParam.stream().findAny();

        if (cookie != null) {
            if (jsonp.isPresent() && !jsonp.get().matches("^[A-Za-z]+$")) {
                throw new RakamException(BAD_REQUEST);
            }

            if (session != null) {
                Integer id = null;
                try {
                    id = extractUserFromCookie(session, encryptionConfig.getSecretKey());
                }
                catch (Exception e) {
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

    private FullHttpResponse unauthorized(Optional<String> jsonp)
    {
        String encode = JsonHelper.jsonObject()
                .put("success", false)
                .put("message", UNAUTHORIZED.reasonPhrase())
                .put("googleApiKey", config.getGoogleClientId()).toString();
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
            @ApiParam("password") String password)
    {
        final Optional<WebUser> user = service.login(email, password);

        if (user.isPresent()) {
            return getLoginResponseForUser(user.get());
        }

        throw new RakamException("Account couldn't found.", HttpResponseStatus.NOT_FOUND);
    }

    @JsonRequest
    @Path("/login_with_google")
    public Response<WebUser> login_with_google(@ApiParam("access_token") String accessToken)
    {
        GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken);
        Oauth2 oauth2 = new Oauth2.Builder(new NetHttpTransport(),
                new JacksonFactory(), credential).setApplicationName("Oauth2").build();
        Userinfoplus userinfo;
        try {
            userinfo = oauth2.userinfo().get().execute();

            if (!userinfo.getVerifiedEmail()) {
                throw new RakamException("The Google email must be verified", BAD_REQUEST);
            }

            Optional<WebUser> userByEmail = service.getUserByEmail(userinfo.getEmail());
            WebUser user = userByEmail.orElseGet(() ->
                    service.createUser(userinfo.getEmail(),
                            userinfo.getGivenName(), null,
                            userinfo.getGender(), userinfo.getLocale(), userinfo.getId()));
            return getLoginResponseForUser(user);
        }
        catch (IOException e) {
            throw new RakamException("Unable to login", INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("/logout")
    public Response<SuccessMessage> logout()
    {
        return Response.ok(SuccessMessage.success()).addCookie("session", "", null, true, -1L, "/", null);
    }

    private Response getLoginResponseForUser(WebUser user)
    {
        final long expiringTimestamp = Instant.now().plus(7, ChronoUnit.DAYS).getEpochSecond();
        final StringBuilder cookieData = new StringBuilder()
                .append(expiringTimestamp).append("|")
                .append(user.id);

        final String secureKey = CryptUtil.encryptWithHMacSHA1(cookieData.toString(), encryptionConfig.getSecretKey());
        cookieData.append('|').append(secureKey);

        return Response.ok(user).addCookie("session", cookieData.toString(),
                null, true, Duration.ofDays(30).getSeconds(), "/", null);
    }

    public static int extractUserFromCookie(String session, String key)
    {
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
