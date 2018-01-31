package org.rakam.ui.user;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Userinfoplus;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.util.CharsetUtil;
import org.rakam.analysis.ApiKeyService;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.Response;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.ProtectEndpoint;
import org.rakam.ui.RakamUIConfig;
import org.rakam.ui.UIPermissionParameterProvider.Project;
import org.rakam.ui.user.WebUser.UserApiKey;
import org.rakam.ui.user.WebUserService.ProjectConfiguration;
import org.rakam.ui.user.WebUserService.UserAccess;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;

import javax.inject.Named;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.cookie.ServerCookieEncoder.STRICT;

@Path("/ui/user")
@IgnoreApi
public class WebUserHttpService
        extends HttpService {
    private final static Logger LOGGER = Logger.get(WebUserHttpService.class);

    private final WebUserService service;
    private final EncryptionConfig encryptionConfig;
    private final RakamUIConfig config;

    @Inject
    public WebUserHttpService(WebUserService service, RakamUIConfig config, EncryptionConfig encryptionConfig) {
        this.service = service;
        this.encryptionConfig = encryptionConfig;
        this.config = config;
    }

    public static Response getLoginResponseForUser(String secretKey, WebUser user, Duration duration) {
        return Response.ok(user).addCookie("session", getCookieForUser(secretKey, user.id),
                null, true, duration.toMillis() / 1000, "/", null);
    }

    public static String getCookieForUser(String secretKey, int userId) {
        final long expiringTimestamp = Instant.now().plus(7, ChronoUnit.DAYS).getEpochSecond();
        final StringBuilder cookieData = new StringBuilder()
                .append(expiringTimestamp).append("|")
                .append(userId);

        final String secureKey = CryptUtil.encryptWithHMacSHA1(cookieData.toString(), secretKey);
        cookieData.append('|').append(secureKey);
        return cookieData.toString();
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

    @JsonRequest
    @Path("/register")
    public Response register(@ApiParam("email") String email,
                             @ApiParam(value = "name", required = false) String name,
                             @ApiParam("password") String password) {
        // TODO: implement captcha https://github.com/VividCortex/angular-recaptcha https://developers.google.com/recaptcha/docs/verify
        // keep a counter for ip in local nodes
        final WebUser user = service.createUser(email, password, name, null, null, null, false);
        return getLoginResponseForUser(encryptionConfig.getSecretKey(), user, config.getCookieDuration());
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/update/password")
    public SuccessMessage updatePassword(@ApiParam("oldPassword") String oldPassword,
                                         @ApiParam("newPassword") String newPassword,
                                         @javax.inject.Named("user_id") Project project) {
        Optional<WebUser> webUser = service.getUser(project.userId);
        if (webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        service.updateUserPassword(project.userId, oldPassword, newPassword);
        return SuccessMessage.success();
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/update/info")
    public SuccessMessage update(
            @ApiParam("name") String name,
            @javax.inject.Named("user_id") Project project) {
        Optional<WebUser> webUser = service.getUser(project.userId);
        if (webUser.get().readOnly) {
            throw new RakamException("User is not allowed to perform this operation", UNAUTHORIZED);
        }

        service.updateUserInfo(project.userId, name);
        return SuccessMessage.success();
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/get-lock-key")
    public String getLockKey(@ApiParam("api_url") String apiUrl,
                             @javax.inject.Named("user_id") Project project) {
        Optional<WebUser> webUser = service.getUser(project.userId);
        if (webUser.get().readOnly) {
            throw new RakamException("User is not allowed to create projects", UNAUTHORIZED);
        }

        return service.getLockKeyForAPI(project.userId, apiUrl);
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/register-project")
    public UserApiKey registerProject(@ApiParam("name") String name,
                                      @ApiParam("api_url") String apiUrl,
                                      @ApiParam(value = "read_key") String readKey,
                                      @ApiParam(value = "write_key", required = false) String writeKey,
                                      @ApiParam(value = "master_key", required = false) String masterKey,
                                      @javax.inject.Named("user_id") Project project) {
        Optional<WebUser> webUser = service.getUser(project.userId);
        if (webUser.get().readOnly) {
            throw new RakamException("User is not allowed to register projects", UNAUTHORIZED);
        }

        return service.registerProject(project.userId, apiUrl, name, readKey, writeKey, masterKey);
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true, requiresProject = false)
    @Path("/get-api-key-owners")
    public List<WebUserService.ProjectKeyPermission> apiKeyOwners(@Named("user_id") Project project, @ApiParam(value = "read_keys") List<String> readKeys) {
        return service.apiKeyOwners(project.project, readKeys);
    }

    @ProtectEndpoint(writeOperation = true)
    @Path("/get-project-owner")
    @GET
    public WebUserService.ProjectOwner projectOwner(@Named("user_id") Project project) {
        return service.getProjectOwner(project.project);
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/delete-project")
    @DELETE
    public SuccessMessage deleteProject(@Named("user_id") Project project) {
        service.deleteProject(project.userId, project.project);
        return SuccessMessage.success();
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/save-api-keys")
    public ApiKeyService.ProjectApiKeys createApiKeys(
            @Named("user_id") Project project,
            @ApiParam("read_key") String readKey,
            @ApiParam("write_key") String writeKey,
            @ApiParam("master_key") String masterKey) {
        service.saveApiKeys(project.userId, project.project, readKey, writeKey, masterKey);
        return ApiKeyService.ProjectApiKeys.create(masterKey, readKey, writeKey);
    }

    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/revoke-api-keys")
    public SuccessMessage revokeApiKeys(@ApiParam("master_key") String key, @Named("user_id") Project project) {
        service.revokeApiKeys(project.userId, project.project, key);
        return SuccessMessage.success();
    }

    @GET
    @ApiOperation(value = "List users who can access to the project")
    @Path("/user-access")
    public List<UserAccess> getUserAccess(@Named("user_id") Project project) {
        return service.getUserAccessForProject(project.userId, project.project);
    }

    @GET
    @ApiOperation(value = "Get project configurations")
    @Path("/project-configuration")
    public ProjectConfiguration getProjectPreferences(@Named("user_id") Project project) {
        return service.getProjectConfigurations(project.project);
    }

    @JsonRequest
    @ApiOperation(value = "Update project configurations")
    @Path("/project-configuration")
    public SuccessMessage updateProjectPreferences(@Named("user_id") Project project, @BodyParam ProjectConfiguration configuration) {
        if (!service.hasMasterAccess(project.project, project.userId)) {
            throw new RakamException("You don't have master access", UNAUTHORIZED);
        }
        service.updateProjectConfigurations(project.project, configuration);
        return SuccessMessage.success();
    }

    @JsonRequest
    @ApiOperation(value = "Recover my password", authorizations = @Authorization(value = "master_key"))
    @Path("/prepare-recover-password")
    public SuccessMessage prepareRecoverPassword(@ApiParam("email") String email) {
        service.prepareRecoverPassword(email);
        return SuccessMessage.success();
    }

    @ApiOperation(value = "Recover my password", authorizations = @Authorization(value = "master_key"))
    @JsonRequest
    @Path("/perform-recover-password")
    public SuccessMessage performRecoverPassword(
            @ApiParam("key") String key,
            @ApiParam("hash") String hash,
            @ApiParam("password") String password) {
        service.performRecoverPassword(key, hash, password);
        return SuccessMessage.success();
    }

    @JsonRequest

    @ApiOperation(value = "Revoke User Access", authorizations = @Authorization(value = "master_key"))
    @Path("/revoke-user-access")
    @ProtectEndpoint(writeOperation = true)
    public List<String> revokeUserAccess(@Named("user_id") Project project,
                                         @ApiParam("email") String email) {
        return service.revokeUserAccess(project.userId, project.project, email);
    }

    @JsonRequest
    @Path("/give-user-access")
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage giveUserAccess(@Named("user_id") Project project,
                                         @ApiParam("email") String email,
                                         @ApiParam(value = "keys") ApiKeyService.ProjectApiKeys keys,
                                         @ApiParam(value = "read_permission") boolean readPermission,
                                         @ApiParam(value = "write_permission") boolean writePermission,
                                         @ApiParam(value = "master_permission") boolean masterPermission,
                                         @ApiParam(value = "active_ui_features") WebUserService.UIFeatures activeUiFeatures) {
        Optional<WebUser> user = service.getUser(project.userId);
        if (!user.get().projects.stream()
                .anyMatch(e -> e.apiKeys.stream().anyMatch(a -> a.masterKey() != null))) {
            throw new RakamException(FORBIDDEN);
        }

        service.giveAccessToUser(project.project, user.get().id, email, keys,
                readPermission, writePermission, masterPermission, activeUiFeatures, Optional.empty());
        return SuccessMessage.success();
    }

    @JsonRequest
    @Path("/update-user-access")
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage updateUserAccess(@Named("user_id") Project project,
                                           @ApiParam("email") String email,
                                           @ApiParam(value = "read_permission") boolean readPermission,
                                           @ApiParam(value = "write_permission") boolean writePermission,
                                           @ApiParam(value = "master_permission") boolean masterPermission,
                                           @ApiParam(value = "active_ui_features") WebUserService.UIFeatures activeUiFeatures) {
        Optional<WebUser> user = service.getUser(project.userId);
        if (!user.get().projects.stream()
                .anyMatch(e -> e.apiKeys.stream().anyMatch(a -> a.masterKey() != null))) {
            throw new RakamException(FORBIDDEN);
        }

        service.giveAccessToExistingUser(project.project, user.get().id, email,
                readPermission, writePermission, masterPermission, activeUiFeatures);
        return SuccessMessage.success();
    }

    @GET
    @JsonRequest
    @Path("/me")
    public void me(RakamHttpRequest request, @CookieParam(value = "session", required = false) String session) {
        List<String> jsonpParam = request.params().get("jsonp");
        Optional<String> jsonp = jsonpParam == null ? Optional.empty() : jsonpParam.stream().findAny();

        if (jsonp.isPresent() && !jsonp.get().matches("^[A-Za-z]+$")) {
            throw new RakamException(BAD_REQUEST);
        }

        if (session != null) {
            Integer id;
            try {
                id = extractUserFromCookie(session, encryptionConfig.getSecretKey());
            } catch (Exception e) {
                request.response(unauthorized(jsonp)).end();
                return;
            }

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

        request.response(unauthorized(jsonp)).end();
    }

    private FullHttpResponse unauthorized(Optional<String> jsonp) {
        String encode = JsonHelper.jsonObject()
                .put("success", false)
                .put("message", UNAUTHORIZED.reasonPhrase())
                .put("authentication", config.getAuthentication())
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
                                   @ApiParam("password") String password) {
        final Optional<WebUser> user = service.login(email, password);

        if (user.isPresent()) {
            return getLoginResponseForUser(encryptionConfig.getSecretKey(), user.get(), config.getCookieDuration());
        }

        throw new RakamException("You have entered an unregistered email or invalid password.", NOT_FOUND);
    }

    @JsonRequest
    @Path("/login_with_google")
    public Response<WebUser> loginWithGoogle(@ApiParam("access_token") String accessToken) {
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
                            null, userinfo.getGivenName(),
                            userinfo.getGender(),
                            userinfo.getLocale(),
                            userinfo.getId(), false));
            return getLoginResponseForUser(encryptionConfig.getSecretKey(), user, config.getCookieDuration());
        } catch (IOException e) {
            LOGGER.error(e);
            throw new RakamException("Unable to login", INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("/logout")
    public Response<SuccessMessage> logout() {
        return Response.ok(SuccessMessage.success()).addCookie("session", "", null, true, -1L, "/", null);
    }

    public String getCookieForUser(int userId) {
        return getCookieForUser(encryptionConfig.getSecretKey(), userId);
    }
}
