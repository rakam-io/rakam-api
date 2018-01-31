package org.rakam.ui.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.lambdaworks.crypto.SCryptUtil;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.Access;
import org.rakam.analysis.ApiKeyService.ProjectApiKeys;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.EncryptionConfig;
import org.rakam.report.EmailClientConfig;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.ui.AuthService;
import org.rakam.ui.RakamUIConfig;
import org.rakam.ui.UIEvents;
import org.rakam.util.*;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.BooleanMapper;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.mail.MessagingException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Charsets.UTF_8;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.HOURS;

public class WebUserService {
    private final static Logger LOGGER = Logger.get(WebUserService.class);
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$");
    private static final Pattern PASSWORD_PATTERN = Pattern.compile("^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).{8,}$");
    private static final Mustache resetPasswordHtmlCompiler;
    private static final Mustache resetPasswordTxtCompiler;
    private static final Mustache welcomeHtmlCompiler;
    private static final Mustache welcomeTxtCompiler;
    private static final Mustache resetPasswordTitleCompiler;
    private static final Mustache welcomeTitleCompiler;
    private static final Mustache userAccessNewMemberTitleCompiler;
    private static final Mustache userAccessNewMemberTxtCompiler;
    private static final Mustache userAccessNewMemberHtmlCompiler;
    private static final Mustache userAccessHtmlCompiler;
    private static final Mustache userAccessTxtCompiler;
    private static final Mustache userAccessTitleCompiler;

    static {
        try {
            MustacheFactory mf = new DefaultMustacheFactory();

            resetPasswordHtmlCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/resetpassword/resetpassword.html"), UTF_8)), "resetpassword.html");
            resetPasswordTxtCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/resetpassword/resetpassword.txt"), UTF_8)), "resetpassword.txt");
            resetPasswordTitleCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/resetpassword/title.txt"), UTF_8)), "resetpassword_title.txt");

            welcomeHtmlCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/welcome/welcome.html"), UTF_8)), "welcome.html");
            welcomeTxtCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/welcome/welcome.txt"), UTF_8)), "welcome.txt");
            welcomeTitleCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/welcome/title.txt"), UTF_8)), "welcome_title.txt");

            userAccessNewMemberHtmlCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/teamaccess_newmember/teamaccess.html"), UTF_8)), "welcome.html");
            userAccessNewMemberTxtCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/teamaccess_newmember/teamaccess.txt"), UTF_8)), "welcome.txt");
            userAccessNewMemberTitleCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/teamaccess_newmember/title.txt"), UTF_8)), "welcome_title.txt");

            userAccessHtmlCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/teamaccess/teamaccess.html"), UTF_8)), "welcome.html");
            userAccessTxtCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/teamaccess/teamaccess.txt"), UTF_8)), "welcome.txt");
            userAccessTitleCompiler = mf.compile(new StringReader(Resources.toString(
                    WebUserService.class.getResource("/mail/teamaccess/title.txt"), UTF_8)), "welcome_title.txt");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private final DBI dbi;
    private final RakamUIConfig config;
    private final EncryptionConfig encryptionConfig;
    private final EventBus eventBus;
    private final EmailClientConfig mailConfig;
    private final AuthService authService;

    @Inject
    public WebUserService(
            @Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource,
            EventBus eventBus,
            com.google.common.base.Optional<AuthService> authService,
            RakamUIConfig config,
            EncryptionConfig encryptionConfig,
            EmailClientConfig mailConfig) {
        dbi = new DBI(dataSource);
        this.eventBus = eventBus;
        this.authService = authService.orNull();
        this.config = config;
        this.encryptionConfig = encryptionConfig;
        this.mailConfig = mailConfig;
    }

    public ProjectConfiguration getProjectConfigurations(int project) {
        try (Connection conn = dbi.open().getConnection()) {
            PreparedStatement ps = conn.prepareStatement("SELECT project, timezone FROM web_user_project WHERE id = ?");
            ps.setInt(1, project);
            ResultSet resultSet = ps.executeQuery();
            if (!resultSet.next()) {
                throw new RakamException("API key is invalid", HttpResponseStatus.FORBIDDEN);
            }
            return new ProjectConfiguration(resultSet.getString(1), resultSet.getString(2));
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public void updateProjectConfigurations(int project, ProjectConfiguration configuration) {
        try (Connection conn = dbi.open().getConnection()) {
            if (configuration.timezone != null) {
                try {
                    ZoneId.of(configuration.timezone);
                } catch (Exception e) {
                    throw new RakamException("Timezone is invalid", BAD_REQUEST);
                }
            }
            PreparedStatement ps = conn.prepareStatement("UPDATE web_user_project SET timezone = ? WHERE id = ?");
            ps.setString(1, configuration.timezone);
            ps.setInt(2, project);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setStripeId(int userId, String stripeId) {
        try (Handle handle = dbi.open()) {
            int execute = handle
                    .createStatement("UPDATE web_user SET stripe_id = :stripeId WHERE id = :userId")
                    .bind("stripeId", stripeId)
                    .bind("userId", userId).execute();
            if (execute != 1) {
                throw new IllegalStateException();
            }
        }
    }

    public List<ProjectKeyPermission> apiKeyOwners(int project, List<String> readKeys) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("select read_key, web_user.email, web_user_api_key.created_at from web_user_api_key \n" +
                    "join web_user_api_key_permission permission on (permission.api_key_id = web_user_api_key.id and permission.read_permission)\n" +
                    "join web_user on (web_user.id = permission.user_id)\n" +
                    "where read_key = any (:keys) and web_user_api_key.project_id = :project")
                    .bind("keys", handle.getConnection().createArrayOf("text", readKeys.toArray()))
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        return new ProjectKeyPermission(r.getString(1), r.getString(2), r.getTimestamp(3).toInstant());
                    }).list();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ProjectOwner getProjectOwner(int project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("select web_user.id, web_user.email from web_user join web_user_project project on (project.user_id = web_user.id) where project.id = :id")
                    .bind("id", project)
                    .map((index, r, ctx) -> {
                        return new ProjectOwner(r.getInt(1), r.getString(2));
                    }).first();
        }
    }

    public WebUser createUser(String email, String password, String name, String gender, String locale, String googleId, boolean external) {
        final String scrypt;
        if (password != null && !external) {
            if (!PASSWORD_PATTERN.matcher(password).matches()) {
                throw new RakamException("Password is not valid. Your password must contain at least one lowercase character, uppercase character and digit and be at least 8 characters. ", BAD_REQUEST);
            }

            if (config.getHashPassword()) {
                password = CryptUtil.encryptWithHMacSHA1(password, encryptionConfig.getSecretKey());
            }
            scrypt = SCryptUtil.scrypt(password, 2 << 14, 8, 1);
        } else if (external) {
            scrypt = password;
        } else {
            if (googleId == null) {
                throw new RakamException("Password id empty", BAD_REQUEST);
            }

            scrypt = null;
        }

        if (!external && !EMAIL_PATTERN.matcher(email).matches()) {
            throw new RakamException("Email is not valid", BAD_REQUEST);
        }

        WebUser webuser = null;

        try (Handle handle = dbi.open()) {
            try {
                int id = handle.createStatement("INSERT INTO web_user (email, password, name, created_at, gender, user_locale, google_id, external) VALUES (:email, :password, :name, now(), :gender, :locale, :googleId, :external)")
                        .bind("email", email)
                        .bind("name", name)
                        .bind("gender", gender)
                        .bind("locale", locale)
                        .bind("external", external)
                        .bind("googleId", googleId)
                        .bind("password", scrypt).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();

                webuser = new WebUser(id, email, name, false, Instant.now(), generateIntercomHash(email), ImmutableList.of());
            } catch (UnableToExecuteStatementException e) {
                Map<String, Object> existingUser = handle.createQuery("SELECT created_at FROM web_user WHERE lower(email) = lower(:email)").bind("email", email).first();
                if (existingUser != null) {
                    if (existingUser.get("created_at") != null) {
                        throw new AlreadyExistsException("A user with same email address", EXPECTATION_FAILED);
                    } else {
                        // somebody gave access for a project to this email address
                        int id = handle.createStatement("UPDATE web_user SET password = :password, name = :name, created_at = now() WHERE lower(email) = lower(:email)")
                                .bind("email", email)
                                .bind("name", name)
                                .bind("password", scrypt).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
                        if (id > 0) {
                            webuser = new WebUser(id, email, name, false, Instant.now(), generateIntercomHash(email), ImmutableList.of());
                        }
                    }
                }

                if (webuser == null) {
                    throw e;
                }
            }
        }

        return webuser;
    }

    public void updateUserInfo(int id, String name) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("UPDATE web_user SET name = :name WHERE id = :id")
                    .bind("id", id)
                    .bind("name", name)
                    .executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
        }
    }

    public String generateIntercomHash(String email) {
        if (config.getIntercomSecretKey() == null) {
            return null;
        }

        try {
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secret_key = new SecretKeySpec(config.getIntercomSecretKey().getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            sha256_HMAC.init(secret_key);

            byte[] hash = (sha256_HMAC.doFinal(email.getBytes()));
            StringBuffer result = new StringBuffer();
            for (byte b : hash) {
                result.append(String.format("%02X", b));
            }

            return result.toString();
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException(e);
        }
    }

    public void updateUserPassword(int id, String oldPassword, String newPassword) {
        final String scrypt = SCryptUtil.scrypt(newPassword, 2 << 14, 8, 1);

        if (!PASSWORD_PATTERN.matcher(newPassword).matches()) {
            throw new RakamException("Password is not valid. Your password must contain at least one lowercase character, uppercase character and digit and be at least 8 characters. ", BAD_REQUEST);
        }

        if (config.getHashPassword()) {
            oldPassword = CryptUtil.encryptWithHMacSHA1(oldPassword, encryptionConfig.getSecretKey());
        }

        try (Handle handle = dbi.open()) {
            String hashedPass = handle.createQuery("SELECT password FROM web_user WHERE id = :id")
                    .bind("id", id).map(StringMapper.FIRST).first();
            if (hashedPass == null) {
                throw new RakamException("User does not exist", BAD_REQUEST);
            }
            if (!SCryptUtil.check(oldPassword, hashedPass)) {
                throw new RakamException("Password is wrong", BAD_REQUEST);
            }
            handle.createStatement("UPDATE web_user SET password = :password WHERE id = :id")
                    .bind("id", id)
                    .bind("password", scrypt).execute();
        }
    }

    public String getLockKeyForAPI(int user, String apiUrl) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT lock_key FROM rakam_cluster WHERE user_id = :userId AND api_url = :apiUrl")
                    .bind("userId", user).bind("apiUrl", apiUrl)
                    .map(StringMapper.FIRST).first();
        }
    }

    public List<String> revokeUserAccess(int userId, int project, String email) {
        try (Handle handle = dbi.open()) {
            if (!hasMasterAccess(handle, project, userId)) {
                throw new RakamException("You do not have master key permission", UNAUTHORIZED);
            }

            List<Map<String, Object>> list = handle.createQuery("SELECT api_key.id, api_key.master_key FROM web_user_api_key_permission permission " +
                    "JOIN web_user_api_key api_key ON (api_key.id = permission.api_key_id) " +
                    "WHERE project_id = :project AND permission.user_id = " +
                    "(SELECT id FROM web_user WHERE lower(email) = lower(:email))")
                    .bind("project", project)
                    .bind("email", email).list();

            if (list.isEmpty()) {
                throw new RakamException(NOT_FOUND);
            }

            handle.createStatement("DELETE FROM web_user_api_key_permission WHERE api_key_id in (" +
                    list.stream().map(a -> a.get("id").toString()).collect(Collectors.joining(", ")) + ")")
                    .execute();

            return list.stream().map(e -> e.get("master_key").toString()).collect(Collectors.toList());
        }
    }

    public void performRecoverPassword(String key, String hash, String newPassword) {
        if (!PASSWORD_PATTERN.matcher(newPassword).matches()) {
            throw new RakamException("Password is not valid. " +
                    "Your password must contain at least one lowercase character, uppercase character and digit and be at least 8 characters. ", BAD_REQUEST);
        }

        String realKey;
        try {
            realKey = new String(Base64.getDecoder().decode(key.getBytes(UTF_8)), UTF_8);
        } catch (IllegalArgumentException e) {
            throw new RakamException("Invalid token", UNAUTHORIZED);
        }
        if (!CryptUtil.encryptWithHMacSHA1(realKey, encryptionConfig.getSecretKey()).equals(hash)) {
            throw new RakamException("Invalid token", UNAUTHORIZED);
        }

        String[] split = realKey.split("\\|", 2);
        if (split.length != 2) {
            throw new RakamException(BAD_REQUEST);
        }
        try {
            if (Instant.ofEpochSecond(Long.parseLong(split[0])).compareTo(Instant.now()) < 0) {
                throw new RakamException("Token expired", UNAUTHORIZED);
            }
        } catch (NumberFormatException e) {
            throw new RakamException("Invalid token", UNAUTHORIZED);
        }

        final String scrypt = SCryptUtil.scrypt(newPassword, 2 << 14, 8, 1);

        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE web_user SET password = :password WHERE lower(email) = lower(:email)")
                    .bind("email", split[1])
                    .bind("password", scrypt).execute();
            if (execute == 0) {
                throw new IllegalStateException();
            }
        }
    }

    public void prepareRecoverPassword(String email) {
        if (!EMAIL_PATTERN.matcher(email).matches()) {
            throw new RakamException("Email is not valid", BAD_REQUEST);
        }

        if (!getUserByEmail(email).isPresent()) {
            throw new RakamException("Email is not found", BAD_REQUEST);
        }
        Map<String, Object> scopes = ImmutableMap.of(
                "product_name", "Rakam",
                "action_url", format("%s/perform-recover-password?%s",
                        mailConfig.getSiteUrl(), getRecoverUrl(email, 3)));

        sendMail(resetPasswordTitleCompiler, resetPasswordTxtCompiler, resetPasswordHtmlCompiler, email, scopes).join();
    }

    private String getRecoverUrl(String email, int hours) {
        long expiration = Instant.now().plus(hours, HOURS).getEpochSecond();
        String key = expiration + "|" + email;
        String hash = CryptUtil.encryptWithHMacSHA1(key, encryptionConfig.getSecretKey());
        String encoded = new String(Base64.getEncoder().encode(key.getBytes(UTF_8)), UTF_8);
        try {
            return format("key=%s&hash=%s", URLEncoder.encode(encoded, "UTF-8"), URLEncoder.encode(hash, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    private CompletableFuture sendMail(Mustache titleCompiler, Mustache contentCompiler, Mustache htmlCompiler, String email, Map<String, Object> data) {
        StringWriter writer;

        writer = new StringWriter();
        contentCompiler.execute(writer, data);
        String txtContent = writer.toString();

        writer = new StringWriter();
        htmlCompiler.execute(writer, data);
        String htmlContent = writer.toString();

        writer = new StringWriter();
        titleCompiler.execute(writer, data);
        String title = writer.toString();
        MailSender mailSender = mailConfig.getMailSender();

        return CompletableFuture.runAsync(() -> {
            try {
                mailSender.sendMail(email, title, txtContent, Optional.of(htmlContent), Stream.empty());
            } catch (MessagingException e) {
                LOGGER.error(e, "Unable to send mail");
            }
        });
    }

    public void deleteProject(int user, int projectId) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM web_user_project WHERE id = :project and user_id = :userId")
                    .bind("userId", user)
                    .bind("project", projectId)
                    .execute();
        }
    }

    public WebUser.UserApiKey registerProject(int user, String apiUrl, String project, String readKey, String writeKey, String masterKey) {
        int projectId;
        try (Handle handle = dbi.open()) {
            try {
                projectId = (Integer) handle.createStatement("INSERT INTO web_user_project " +
                        "(project, api_url, user_id) " +
                        "VALUES (:project, :apiUrl, :userId)")
                        .bind("userId", user)
                        .bind("project", project)
                        .bind("apiUrl", apiUrl)
                        .executeAndReturnGeneratedKeys().first().get("id");
            } catch (Exception e) {
                if (e.getMessage().contains("project_check")) {
                    throw new RakamException("Project already exists.", BAD_REQUEST);
                }
                throw e;
            }

            handle.createStatement("INSERT INTO web_user_api_key " +
                    "(user_id, project_id, read_key, write_key, master_key) " +
                    "VALUES (:userId, :project, :readKey, :writeKey, :masterKey)")
                    .bind("userId", user)
                    .bind("project", projectId)
                    .bind("readKey", readKey)
                    .bind("writeKey", writeKey)
                    .bind("masterKey", masterKey)
                    .execute();
        }

        eventBus.post(new UIEvents.ProjectCreatedEvent(projectId));
        return new WebUser.UserApiKey(projectId, readKey, writeKey, masterKey);
    }

    public List<UserAccess> getUserAccessForProject(int user, int project) {
        try (Handle handle = dbi.open()) {
            if (!hasMasterAccess(handle, project, user)) {
                throw new RakamException("You do not have master key permission", UNAUTHORIZED);
            }

            return handle.createQuery("SELECT web_user.email, keys.scope_expression, " +
                    "read_permission, write_permission, master_permission, web_user.id, active_ui_features " +
                    "FROM web_user_api_key_permission keys " +
                    "JOIN web_user_api_key ON (web_user_api_key.id = keys.api_key_id) " +
                    "JOIN web_user ON (web_user.id = keys.user_id) " +
                    "WHERE web_user.id != :user AND web_user_api_key.project_id = :project " +
                    "ORDER BY keys.created_at")
                    .bind("user", user)
                    .bind("project", project).map((i, resultSet, statementContext) ->
                            new UserAccess(project, resultSet.getInt(6), resultSet.getString(1), resultSet.getString(2),
                                    resultSet.getBoolean(3), resultSet.getBoolean(4), resultSet.getBoolean(5),
                                    resultSet.getString(7) == null ? null : JsonHelper.read(resultSet.getString(7), UIFeatures.class))).list();
        }
    }

    public static class UIFeatures {
        public final boolean eventExplorer;
        public final boolean dashboard;
        public final boolean reports;
        public final boolean people;
        public final boolean funnel;
        public final boolean retention;

        @JsonCreator
        public UIFeatures(@ApiParam(value = "eventExplorer", required = false) boolean eventExplorer,
                          @ApiParam(value = "dashboard", required = false) boolean dashboard,
                          @ApiParam(value = "reports", required = false) boolean reports,
                          @ApiParam(value = "people", required = false) boolean people,
                          @ApiParam(value = "funnel", required = false) boolean funnel,
                          @ApiParam(value = "retention", required = false) boolean retention) {
            this.eventExplorer = eventExplorer;
            this.dashboard = dashboard;
            this.reports = reports;
            this.people = people;
            this.funnel = funnel;
            this.retention = retention;
        }
    }

    public void giveAccessToExistingUser(int projectId, int userId, String email, boolean readPermission, boolean writePermission, boolean masterPermission, UIFeatures activeUiFeatures) {
        try (Handle handle = dbi.open()) {
            if (!hasMasterAccess(handle, projectId, userId)) {
                throw new RakamException("You do not have master key permission", UNAUTHORIZED);
            }

            Integer newUserId = handle.createQuery("SELECT id FROM web_user WHERE lower(email) = lower(:email)").bind("email", email)
                    .map(IntegerMapper.FIRST).first();

            if (newUserId == null) {
                throw new RakamException(NOT_FOUND);
            }

            int exists = handle.createStatement("UPDATE web_user_api_key_permission SET " +
                    "read_permission = :readPermission, write_permission = :writePermission, master_permission = :masterPermission, active_ui_features = :activeUiFeatures " +
                    "WHERE user_id = :newUserId")
                    .bind("newUserId", newUserId)
                    .bind("readPermission", readPermission)
                    .bind("writePermission", writePermission)
                    .bind("masterPermission", masterPermission)
                    .bind("activeUiFeatures", JsonHelper.encode(activeUiFeatures))
                    .bind("project", projectId).execute();

            if (exists == 0) {
                throw new RakamException(NOT_FOUND);
            }
        }
    }

    public void sendNewUserMail(String project, String email) {
        sendMail(userAccessNewMemberTitleCompiler, userAccessNewMemberTxtCompiler, userAccessNewMemberHtmlCompiler, email, ImmutableMap.of(
                "product_name", "Rakam",
                "project", project,
                "action_url", format("%s/perform-recover-password?%s",
                        mailConfig.getSiteUrl(), getRecoverUrl(email, 24))));
    }

    public void giveAccessToUser(int projectId, int userId, String email, ProjectApiKeys keys,
                                 boolean readPermission, boolean writePermission, boolean masterPermission, UIFeatures activeUiFeatures,
                                 Optional<Access> access) {
        if (masterPermission && access.isPresent()) {
            throw new RakamException("Scoped keys cannot have access to master_key", BAD_REQUEST);
        }
        if (this.getProjectOwner(projectId).email.equals(email)) {
            throw new RakamException("The user is already the owner of project", UNAUTHORIZED);
        }
        ProjectConfiguration projectConfigurations = getProjectConfigurations(projectId);

        Integer newUserId;
        try (Handle handle = dbi.open()) {
            if (!hasMasterAccess(handle, projectId, userId)) {
                throw new RakamException("You do not have master key permission", UNAUTHORIZED);
            }

            try {
                newUserId = handle.createStatement("INSERT INTO web_user (email, created_at) VALUES (:email, now())")
                        .bind("email", email).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();

                sendNewUserMail(projectConfigurations.name, email);
            } catch (Exception e) {
                Map.Entry<Integer, Boolean> element = handle.createQuery("SELECT id, password is null FROM web_user WHERE lower(email) = lower(:email)").bind("email", email)
                        .map((ResultSetMapper<Map.Entry<Integer, Boolean>>) (index, r, ctx) -> new AbstractMap.SimpleImmutableEntry<>(r.getInt(1), r.getBoolean(2))).first();
                newUserId = element.getKey();

                boolean passwordIsNull = element.getValue();

                if (passwordIsNull) {
                    sendNewUserMail(projectConfigurations.name, email);
                } else {
                    sendMail(userAccessTitleCompiler, userAccessTxtCompiler, userAccessHtmlCompiler, email, ImmutableMap.of(
                            "product_name", "Rakam",
                            "project", projectConfigurations.name,
                            "action_url", mailConfig.getSiteUrl()));
                }
            }
        }

        if (newUserId == null) {
            throw new IllegalStateException("User id cannot be found.");
        }

        final int finalNewUserId = newUserId;
        try {
            dbi.inTransaction((Handle handle, TransactionStatus transactionStatus) -> {
                Integer apiKeyId = saveApiKeys(handle, userId, projectId, keys.readKey(), keys.writeKey(), keys.masterKey());

                boolean exists = handle.createQuery("SELECT true FROM web_user_api_key_permission " +
                        "WHERE user_id = :newUserId AND api_key_id IN (SELECT id FROM web_user_api_key WHERE project_id = :project)")
                        .bind("newUserId", finalNewUserId)
                        .bind("readPermission", readPermission)
                        .bind("writePermission", writePermission)
                        .bind("masterPermission", masterPermission)
                        .bind("project", projectId).first() != null;

                if (!exists) {
                    handle.createStatement("INSERT INTO web_user_api_key_permission (api_key_id, user_id, read_permission, write_permission, master_permission, active_ui_features) " +
                            " VALUES (:apiKeyId, :newUserId, :readPermission, :writePermission, :masterPermission, :activeUiFeatures)")
                            .bind("apiKeyId", apiKeyId)
                            .bind("newUserId", finalNewUserId)
                            .bind("readPermission", readPermission)
                            .bind("writePermission", writePermission)
                            .bind("masterPermission", masterPermission)
                            .bind("activeUiFeatures", JsonHelper.encode(activeUiFeatures))
                            .execute();
                } else {
                    throw new RakamException("The user (" + email + ") already has access", BAD_REQUEST);
                }

                return null;
            });
        } catch (CallbackFailedException e) {
            if (e.getCause() instanceof RakamException) {
                throw (RakamException) e.getCause();
            }
        }
    }

    public Integer saveApiKeys(int user, int projectId, String readKey, String writeKey, String masterKey) {
        try (Handle handle = dbi.open()) {
            return saveApiKeys(handle, user, projectId, readKey, writeKey, masterKey);
        }
    }

    public Integer saveApiKeys(Handle handle, int user, int projectId, String readKey, String writeKey, String masterKey) {
        if (!hasMasterAccess(handle, projectId, user)) {
            throw new RakamException("You do not have master key permission", UNAUTHORIZED);
        }

        return handle.createStatement("INSERT INTO web_user_api_key " +
                "(user_id, project_id, read_key, write_key, master_key) " +
                "VALUES (:userId, :project, :readKey, :writeKey, :masterKey)")
                .bind("userId", user)
                .bind("project", projectId)
                .bind("readKey", readKey)
                .bind("writeKey", writeKey)
                .bind("masterKey", masterKey)
                .executeAndReturnGeneratedKeys((index, r, ctx) -> r.getInt("id")).first();
    }

    private boolean hasMasterAccess(Handle handle, int project, int user) {
        return TRUE.equals(handle.createQuery("select user_id = :user or (select bool_or(master_permission) from web_user_api_key_permission p " +
                "join web_user_api_key a on (p.api_key_id = a.id) where p.user_id = :user and a.project_id = :project) from web_user_project where id = :project")
                .bind("user", user)
                .bind("project", project).map(BooleanMapper.FIRST)
                .first());
    }

    public boolean hasMasterAccess(int project, int user) {
        try (Handle handle = dbi.open()) {
            return hasMasterAccess(handle, project, user);
        }
    }

    public Optional<WebUser> login(String email, String password) {
        if (config.getHashPassword()) {
            password = CryptUtil.encryptWithHMacSHA1(password, encryptionConfig.getSecretKey());
        }

        List<WebUser.Project> projects;

        final Map<String, Object> data;
        String passwordInDb;
        try (Handle handle = dbi.open()) {
            data = handle
                    .createQuery("SELECT id, name, password, read_only, created_at FROM web_user WHERE lower(email) = lower(:email)")
                    .bind("email", email).first();

            if (authService != null) {
                boolean login = authService.login(email, password);
                if (login && data == null) {
                    WebUser user = createUser(email, password, null, null, null, null, true);
                    return Optional.of(user);
                } else if (!login) {
                    return Optional.empty();
                }
            }

            if (data == null) {
                return Optional.empty();
            }
        }

        passwordInDb = (String) data.get("password");

        if (config.getAuthentication() != null) {
            if (!Objects.equals(password, passwordInDb)) {
                try (Handle handle = dbi.open()) {
                    int updated = handle
                            .createStatement("UPDATE web_user SET password = :password WHERE lower(email) = lower(:email)")
                            .bind("password", password).execute();
                    if (updated == 0) {
                        throw new IllegalStateException();
                    }
                }
            }
        } else {
            if (passwordInDb == null) {
                throw new RakamException("Your password is not set. Please reset your password in order to set it.",
                        PRECONDITION_REQUIRED);
            }

            if (!SCryptUtil.check(password, passwordInDb)) {
                return Optional.empty();
            }
        }

        try (Handle handle = dbi.open()) {
            String name = (String) data.get("name");
            int id = (int) data.get("id");
            boolean readOnly = (boolean) data.get("read_only");
            Timestamp createdAt = (Timestamp) data.get("created_at");
            projects = getUserApiKeys(handle, id);
            return Optional.of(new WebUser(id, email, name, readOnly, createdAt.toInstant(), generateIntercomHash(email), projects));
        }
    }

    public Optional<WebUser> getUserByEmail(String email) {
        List<WebUser.Project> projectDefinitions;

        try (Handle handle = dbi.open()) {
            final Map<String, Object> data = handle
                    .createQuery("SELECT id, name, read_only, created_at FROM web_user WHERE lower(email) = lower(:email)")
                    .bind("email", email).first();
            if (data == null) {
                return Optional.empty();
            }
            String name = (String) data.get("name");
            int id = (int) data.get("id");
            boolean readOnly = (boolean) data.get("read_only");
            Timestamp createdAt = (Timestamp) data.get("created_at");

            projectDefinitions = getUserApiKeys(handle, id);
            return Optional.of(new WebUser(id, email, name, readOnly, createdAt.toInstant(), generateIntercomHash(email), projectDefinitions));
        }
    }

    public Optional<WebUser> getUser(int id) {
        List<WebUser.Project> projectDefinitions;

        try (Handle handle = dbi.open()) {
            final Map<String, Object> data = handle
                    .createQuery("SELECT id, name, email, read_only, created_at FROM web_user WHERE id = :id")
                    .bind("id", id).first();
            if (data == null) {
                return Optional.empty();
            }
            String name = (String) data.get("name");
            String email = (String) data.get("email");
            Timestamp createdAt = (Timestamp) data.get("created_at");
            id = (int) data.get("id");

            projectDefinitions = getUserApiKeys(handle, id);
            return Optional.of(new WebUser(id, email, name,
                    (Boolean) data.get("read_only"), createdAt.toInstant(), generateIntercomHash(email), projectDefinitions));
        }
    }

    public String getUserStripeId(int id) {
        try (Handle handle = dbi.open()) {
            return handle
                    .createQuery("SELECT stripe_id FROM web_user WHERE id = :id")
                    .bind("id", id).map(StringMapper.FIRST).first();
        }
    }

    private List<WebUser.Project> getUserApiKeys(Handle handle, int userId) {
        List<WebUser.Project> list = new ArrayList<>();
        ResultIterator<Object> user = handle.createQuery("SELECT project.id, project.project, project.api_url, project.timezone, " +
                ":user, api_key.master_key, api_key.read_key, api_key.write_key, null " +
                " FROM web_user_project project " +
                " JOIN web_user_api_key api_key ON (api_key.project_id = project.id)" +
                " WHERE api_key.user_id = :user " +
                " UNION ALL SELECT api_key.project_id, project.project, project.api_url, project.timezone, project.user_id, " +
                "case when permission.master_permission then api_key.master_key else null end," +
                "case when permission.master_permission or permission.read_permission then api_key.read_key else null end," +
                "case when permission.master_permission or permission.write_permission then api_key.write_key else null end, permission.active_ui_features " +
                "FROM web_user_api_key_permission permission \n" +
                "JOIN web_user_api_key api_key ON (permission.api_key_id = api_key.id) \n" +
                "JOIN web_user_project project ON (project.id = api_key.project_id)\n" +
                "WHERE permission.user_id = :user" +
                " ORDER BY id NULLS LAST")
                .bind("user", userId)
                .map((index, r, ctx) -> {
                    int id = r.getInt(1);
                    String name = r.getString(2);
                    String url = r.getString(3);
                    ZoneId zoneId;
                    try {
                        zoneId = r.getString(4) != null ? ZoneId.of(r.getString(4)) : null;
                    } catch (ZoneRulesException e) {
                        zoneId = null;
                    }

                    int ownerId = r.getInt(5);
                    UIFeatures uiFeatures = r.getString(9) == null ? null : JsonHelper.read(r.getString(9), UIFeatures.class);

                    ZoneId finalZoneId = zoneId;
                    WebUser.Project p = list.stream().filter(e -> e.id == id)
                            .findFirst()
                            .orElseGet(() -> {
                                WebUser.Project project = new WebUser.Project(id, name, url, finalZoneId, ownerId, new ArrayList<>(), uiFeatures);
                                list.add(project);
                                return project;
                            });
                    p.apiKeys.add(ProjectApiKeys.create(r.getString(6), r.getString(7), r.getString(8)));
                    return null;
                }).iterator();

        while (user.hasNext()) {
            user.next();
        }

        return list;
    }

    public void revokeApiKeys(int user, int project, String masterKey) {
        try (Handle handle = dbi.open()) {
            if (!hasMasterAccess(handle, project, user)) {
                throw new RakamException("You do not have master key permission", UNAUTHORIZED);
            }

            try {
                handle.createStatement("DELETE FROM web_user_api_key " +
                        "WHERE user_id = :user_id AND project_id = :project AND master_key = :masterKey")
                        .bind("user_id", user)
                        .bind("project", project)
                        .bind("masterKey", masterKey).execute();
            } catch (Throwable e) {
                if (e.getMessage().contains("web_user_api_key_permission")) {
                    List<String> list = handle.createQuery("SELECT web_user.email FROM web_user_api_key_permission permission " +
                            "JOIN web_user ON (web_user.id = permission.user_id) " +
                            "WHERE api_key_id in (SELECT id FROM web_user_api_key WHERE master_key = :masterKey and user_id = :userId and project_id = :project)")
                            .bind("masterKey", masterKey).bind("userId", user).bind("project", project).map(StringMapper.FIRST).list();

                    if (!list.isEmpty()) {
                        throw new RakamException("Users [" + list.stream().collect(Collectors.joining(", ")) + "] use this key." +
                                " You need to revoke the access of the user in order to be able to delete this key", BAD_REQUEST);
                    }
                }

                throw e;
            }
        }
    }

    public static class ProjectConfiguration {
        public final String name;
        public final String timezone;

        @JsonCreator
        public ProjectConfiguration(@ApiParam(value = "timezone", required = false) String timezone) {
            this(null, timezone);
        }

        public ProjectConfiguration(String name, String timezone) {
            this.timezone = timezone;
            this.name = name;
        }
    }

    public static class ProjectKeyPermission {
        public final String readKey;
        public final String email;
        public final Instant createdAt;

        public ProjectKeyPermission(String readKey, String email, Instant createdAt) {
            this.readKey = readKey;
            this.email = email;
            this.createdAt = createdAt;
        }
    }

    public static class ProjectOwner {
        public final int id;
        public final String email;

        public ProjectOwner(int id, String email) {
            this.id = id;
            this.email = email;
        }
    }

    public static class UserAccess {
        @JsonProperty("project")
        public final int project;
        @JsonProperty("id")
        public final int id;
        @JsonProperty("email")
        public final String email;
        @JsonProperty("scope_expression")
        public final String scope_expression;
        @JsonProperty("read_key")
        public final boolean readKey;
        @JsonProperty("write_key")
        public final boolean writeKey;
        @JsonProperty("master_key")
        public final boolean masterKey;
        @JsonProperty("active_ui_features")
        public final UIFeatures activeUIFeatures;

        public UserAccess(int project, int id, String email, String scope_expression, boolean readKey, boolean writeKey, boolean masterKey, UIFeatures activeUIFeatures) {
            this.project = project;
            this.id = id;
            this.email = email;
            this.scope_expression = scope_expression;
            this.readKey = readKey;
            this.writeKey = writeKey;
            this.masterKey = masterKey;
            this.activeUIFeatures = activeUIFeatures;
        }
    }

    public static final class Project {
        public final String project;
        public final String apiUrl;

        public Project(String project, String apiUrl) {
            this.project = project;
            this.apiUrl = apiUrl;
        }
    }
}
