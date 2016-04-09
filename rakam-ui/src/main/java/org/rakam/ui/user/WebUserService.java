package org.rakam.ui.user;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.lambdaworks.crypto.SCryptUtil;
import io.airlift.log.Logger;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ApiKeyService.ProjectApiKeys;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.EncryptionConfig;
import org.rakam.report.EmailClientConfig;
import org.rakam.ui.RakamUIConfig;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.CryptUtil;
import org.rakam.util.MailSender;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.mail.MessagingException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import static com.google.common.base.Charsets.UTF_8;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class WebUserService {
    private final static Logger LOGGER = Logger.get(WebUserService.class);

    private final DBI dbi;
    private final Metastore metastore;
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$");
    private static final Pattern PASSWORD_PATTERN = Pattern.compile("^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).{8,}$");
    private final RakamUIConfig config;
    private final EncryptionConfig encryptionConfig;
    private final ApiKeyService apiKeyService;
    private MailSender mailSender;
    private final EmailClientConfig mailConfig;

    private static final Mustache resetPasswordHtmlCompiler;
    private static final Mustache resetPasswordTxtCompiler;
    private static final Mustache welcomeHtmlCompiler;
    private static final Mustache welcomeTxtCompiler;
    private static final Mustache resetPasswordTitleCompiler;
    private static final Mustache welcomeTitleCompiler;

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
                    WebUserService.class.getResource("/mail/welcome/welcome.txt"), UTF_8)), "welcome_title.txt");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Inject
    public WebUserService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
                          Metastore metastore,
                          ApiKeyService apiKeyService,
                          RakamUIConfig config,
                          EncryptionConfig encryptionConfig,
                          EmailClientConfig mailConfig) {
        dbi = new DBI(dataSource);
        this.metastore = metastore;
        this.config = config;
        this.apiKeyService = apiKeyService;
        this.encryptionConfig = encryptionConfig;
        this.mailConfig = mailConfig;
    }

    public WebUser createUser(String email, String password, String name) {
        if (!PASSWORD_PATTERN.matcher(password).matches()) {
            throw new RakamException("Password is not valid. Your password must contain at least one lowercase character, uppercase character and digit and be at least 8 characters. ", BAD_REQUEST);
        }

        if (config.getHashPassword()) {
            password = CryptUtil.encryptWithHMacSHA1(password, encryptionConfig.getSecretKey());
        }
        final String scrypt = SCryptUtil.scrypt(password, 2 << 14, 8, 1);

        if (!EMAIL_PATTERN.matcher(email).matches()) {
            throw new RakamException("Email is not valid", BAD_REQUEST);
        }

        Map<String, Object> scopes = ImmutableMap.of("product_name", "Rakam");

        WebUser webuser = null;

        try (Handle handle = dbi.open()) {
            try {
                int id = handle.createStatement("INSERT INTO web_user (email, password, name, created_at) VALUES (:email, :password, :name, now())")
                        .bind("email", email)
                        .bind("name", name)
                        .bind("password", scrypt).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
                webuser = new WebUser(id, email, name, ImmutableMap.of());
            } catch (UnableToExecuteStatementException e) {
                Map<String, Object> existingUser = handle.createQuery("SELECT created_at FROM web_user WHERE email = :email").bind("email", email).first();
                if (existingUser != null) {
                    if (existingUser.get("created_at") != null) {
                        throw new AlreadyExistsException("A user with same email address", EXPECTATION_FAILED);
                    } else {
                        // somebody gave access for a project to this email address
                        int id = handle.createStatement("UPDATE web_user SET password = :password, name = :name, created_at = now() WHERE email = :email")
                                .bind("email", email)
                                .bind("name", name)
                                .bind("password", scrypt).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
                        if (id > 0) {
                            webuser = new WebUser(id, email, name, ImmutableMap.of());
                        }
                    }
                }

                if (webuser == null) {
                    throw e;
                }
            }
        }

        sendMail(welcomeTitleCompiler, welcomeTxtCompiler, welcomeHtmlCompiler, email, scopes);

        return webuser;
    }

    public void updateUserInfo(int id, String name) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("UPDATE web_user SET name = :name WHERE id = :id")
                    .bind("id", id)
                    .bind("name", name).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
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

    public WebUser.UserApiKey createProject(int user, String project) {
        if (metastore.getProjects().contains(project)) {
            throw new RakamException("Project already exists", BAD_REQUEST);
        }
        metastore.createProject(project);
        final ProjectApiKeys apiKeys = apiKeyService.createApiKeys(project);
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO web_user_project " +
                    "(id, user_id, project, has_read_permission, has_write_permission, is_admin) " +
                    "VALUES (:id, :userId, :project, true, true, true)")
                    .bind("id", apiKeys.id)
                    .bind("userId", user)
                    .bind("project", project).execute();
        }

        return new WebUser.UserApiKey(apiKeys.readKey, apiKeys.writeKey, apiKeys.masterKey);
    }

    public void revokeUserAccess(String project, String email) {
        List<Integer> integers;
        try (Handle handle = dbi.open()) {
            integers = handle.createStatement("DELETE FROM web_user_project WHERE project = :project AND user_id = (SELECT id FROM web_user WHERE email = :email)")
                    .bind("project", project)
                    .bind("email", email)
                    .bind("project", project)
                    .executeAndReturnGeneratedKeys(IntegerMapper.FIRST).list();
        }
        for (Integer integer : integers) {
            apiKeyService.revokeApiKeys(project, integer);
        }
    }

    public void performRecoverPassword(String key, String hash, String newPassword) {
        if (!PASSWORD_PATTERN.matcher(newPassword).matches()) {
            throw new RakamException("Password is not valid. " +
                    "Your password must contain at least one lowercase character, uppercase character and digit and be at least 8 characters. ", BAD_REQUEST);
        }

        String realKey = new String(Base64.getDecoder().decode(key.getBytes(UTF_8)), UTF_8);
        if (!CryptUtil.encryptWithHMacSHA1(realKey, encryptionConfig.getSecretKey()).equals(hash)) {
            throw new RakamException("Invalid token", UNAUTHORIZED);
        }

        String[] split = realKey.split("|", 2);
        if (split.length != 2) {
            throw new RakamException(BAD_REQUEST);
        }
        try {
            if (Instant.ofEpochSecond(Long.parseLong(split[0])).compareTo(Instant.now()) > 0) {
                throw new RakamException("Token expired", UNAUTHORIZED);
            }
        } catch (NumberFormatException e) {
            throw new RakamException("Invalid token", UNAUTHORIZED);
        }

        final String scrypt = SCryptUtil.scrypt(newPassword, 2 << 14, 8, 1);

        try (Handle handle = dbi.open()) {
            handle.createStatement("UPDATE web_user SET password = :password WHERE email = :email AND password = :password")
                    .bind("email", split[1])
                    .bind("password", scrypt).execute();
        }
    }

    public void prepareRecoverPassword(String email) {
        if (!EMAIL_PATTERN.matcher(email).matches()) {
            throw new RakamException("Email is not valid", BAD_REQUEST);
        }
        long expiration = Instant.now().plus(3, ChronoUnit.HOURS).getEpochSecond();
        String key = expiration + "|" + email;
        String hash = CryptUtil.encryptWithHMacSHA1(key, encryptionConfig.getSecretKey());
        String encoded = new String(Base64.getEncoder().encode(key.getBytes(UTF_8)), UTF_8);

        Map<String, Object> scopes;
        try {
            scopes = ImmutableMap.of("product_name", "Rakam",
                    "action_url", String.format("%s/perform-recover-password?key=%s&hash=%s",
                            mailConfig.getSiteUrl(), URLEncoder.encode(encoded, "UTF-8"), URLEncoder.encode(hash, "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }

        sendMail(resetPasswordTitleCompiler, resetPasswordTxtCompiler, resetPasswordHtmlCompiler, email, scopes).join();
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

        if (mailSender == null) {
            synchronized (this) {
                mailSender = mailConfig.getMailSender();
            }
        }

        return CompletableFuture.runAsync(() -> {
            try {
                mailSender.sendMail(email, title, txtContent, Optional.of(htmlContent));
            } catch (MessagingException e) {
                LOGGER.error(e, "Unable to send mail");
                throw new RakamException("Unable to send mail", INTERNAL_SERVER_ERROR);
            }
        });
    }

    public static class UserAccess {
        public final String email;
        public final String scope_expression;
        public final boolean has_read_permission;
        public final boolean has_write_permission;
        public final boolean is_admin;

        public UserAccess(String email, String scope_expression, boolean has_read_permission, boolean has_write_permission, boolean is_admin) {
            this.email = email;
            this.scope_expression = scope_expression;
            this.has_read_permission = has_read_permission;
            this.has_write_permission = has_write_permission;
            this.is_admin = is_admin;
        }
    }

    public Map<String, List<UserAccess>> getUserAccessForAllProjects(int user) {
        try (Handle handle = dbi.open()) {
            Map<String, List<UserAccess>> map = new HashMap<>();
            handle.createQuery("SELECT project, web_user.email, scope_expression, has_read_permission, has_write_permission, is_admin FROM web_user_project " +
                    "JOIN web_user ON (web_user.id = web_user_project.user_id) WHERE project IN " +
                    "(SELECT DISTINCT project FROM web_user_project WHERE user_id = :user AND is_admin)")
                    .bind("user", user).map((i, resultSet, statementContext) -> {
                return new SimpleImmutableEntry<>(resultSet.getString(1),
                        new UserAccess(resultSet.getString(2), resultSet.getString(3),
                                resultSet.getBoolean(4), resultSet.getBoolean(5), resultSet.getBoolean(6)));
            }).list().forEach(item -> map.computeIfAbsent(item.getKey(), (key) -> new ArrayList<>()).add(item.getValue()));
            return map;
        }
    }

    public void giveAccessToUser(String project, String email, String scopePermission, boolean hasReadPermission, boolean hasWritePermission, boolean isAdmin) {
        Integer userId;
        try (Handle handle = dbi.open()) {
            userId = handle.createQuery("SELECT id FROM web_user WHERE email = :email").bind("email", email)
                    .map(IntegerMapper.FIRST).first();
            if (userId == null) {
                userId = handle.createStatement("INSERT INTO web_user (email) VALUES (:email)")
                        .bind("email", email).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
            }
        }

        final ProjectApiKeys apiKeys = apiKeyService.createApiKeys(project);

        try (Handle handle = dbi.open()) {
            int affectedRows = handle.createStatement("UPDATE web_user_project SET has_read_permission = :read, " +
                    " has_write_permission = :write, scope_expression = :scope, is_admin = :admin" +
                    " WHERE project = :project AND user_id = :user")
                    .bind("user", userId)
                    .bind("project", project)
                    .bind("scope", scopePermission)
                    .bind("admin", isAdmin)
                    .bind("write", hasWritePermission)
                    .bind("read", hasReadPermission).execute();
            if (affectedRows == 0) {
                handle.createStatement("INSERT INTO web_user_project " +
                        "(id, user_id, project, has_read_permission, has_write_permission, scope_expression, is_admin) " +
                        "VALUES (:id, :userId, :project, :hasReadPermission, :hasWritePermission, :scopePermission, :isAdmin)")
                        .bind("id", apiKeys.id)
                        .bind("userId", userId)
                        .bind("hasReadPermission", hasReadPermission)
                        .bind("hasWritePermission", hasWritePermission)
                        .bind("scopePermission", scopePermission)
                        .bind("isAdmin", isAdmin)
                        .bind("project", project).execute();
            }
        }
    }

    public ProjectApiKeys createApiKeys(int user, String project) {
        if (!metastore.getProjects().contains(project)) {
            throw new RakamException("Project does not exists", BAD_REQUEST);
        }

        try (Handle handle = dbi.open()) {
            if (!getUserApiKeys(handle, user).stream().anyMatch(a -> a.project.equals(project))) {
                // TODO: check scope permission keys
                throw new RakamException(UNAUTHORIZED);
            }

            final ProjectApiKeys apiKeys = apiKeyService.createApiKeys(project);

            handle.createStatement("INSERT INTO web_user_project " +
                    "(id, user_id, project, has_read_permission, has_write_permission, is_admin) " +
                    "VALUES (:id, :userId, :project, true, true, true)")
                    .bind("id", apiKeys.id)
                    .bind("userId", user)
                    .bind("project", project).execute();

            return apiKeys;
        }
    }


    public Optional<WebUser> login(String email, String password) {
        String hashedPassword;
        String name;
        int id;

        if (config.getHashPassword()) {
            password = CryptUtil.encryptWithHMacSHA1(password, encryptionConfig.getSecretKey());
        }

        List<ProjectPermission> projects;

        try (Handle handle = dbi.open()) {
            final Map<String, Object> data = handle
                    .createQuery("SELECT id, name, password FROM web_user WHERE email = :email")
                    .bind("email", email).first();
            if (data == null) {
                return Optional.empty();
            }
            hashedPassword = (String) data.get("password");
            name = (String) data.get("name");
            id = (int) data.get("id");

            // TODO move this heavy operation outside of the connection scope.
            if (!SCryptUtil.check(password, hashedPassword)) {
                return Optional.empty();
            }

            projects = getUserApiKeys(handle, id);
        }

        return Optional.of(new WebUser(id, email, name, transformPermissions(projects)));
    }


    public Optional<WebUser> getUser(int id) {
        String name;
        String email;

        List<ProjectPermission> projectPermissions;

        try (Handle handle = dbi.open()) {
            final Map<String, Object> data = handle
                    .createQuery("SELECT id, name, email FROM web_user WHERE id = :id")
                    .bind("id", id).first();
            if (data == null) {
                return Optional.empty();
            }
            name = (String) data.get("name");
            email = (String) data.get("email");
            id = (int) data.get("id");

            projectPermissions = getUserApiKeys(handle, id);
        }

        Map<String, List<ProjectApiKeys>> projects = transformPermissions(projectPermissions);

        return Optional.of(new WebUser(id, email, name, projects));
    }

    public static class ProjectPermission {
        public final int id;
        public final String project;
        public final String scope_expression;
        public final boolean has_read_permission;
        public final boolean has_write_permission;
        public final boolean is_admin;

        public ProjectPermission(int id, String project, String scope_expression, boolean has_read_permission, boolean has_write_permission, boolean is_admin) {
            this.id = id;
            this.project = project;
            this.scope_expression = scope_expression;
            this.has_read_permission = has_read_permission;
            this.has_write_permission = has_write_permission;
            this.is_admin = is_admin;
        }
    }

    private List<ProjectPermission> getUserApiKeys(Handle handle, int userId) {
        return handle.createQuery("SELECT id, project, scope_expression, has_read_permission, has_write_permission, is_admin FROM web_user_project WHERE user_id = :userId")
                .bind("userId", userId)
                .map((i, r, statementContext) ->
                        new ProjectPermission(r.getInt(1), r.getString(2), r.getString(3), r.getBoolean(4), r.getBoolean(5), r.getBoolean(6)))
                .list();
    }

    private Map<String, List<ProjectApiKeys>> transformPermissions(List<ProjectPermission> permissions) {

        Map<String, List<ProjectApiKeys>> projects = Maps.newHashMap();

        final List<ProjectApiKeys> apiKeys = apiKeyService
                .getApiKeys(permissions.stream().mapToInt(row -> row.id).toArray());

        for (ProjectApiKeys apiKey : apiKeys) {
            final ProjectPermission keyProps = permissions.stream().filter(key -> key.id == apiKey.id).findAny().get();
            if (keyProps.scope_expression != null) {
                //TODO generate scoped key
                throw new UnsupportedOperationException();
            }

            String masterKey = Boolean.TRUE.equals(keyProps.is_admin) ? apiKey.masterKey : null;
            String readKey = Boolean.TRUE.equals(keyProps.has_read_permission) ? apiKey.readKey : null;
            String writeKey = Boolean.TRUE.equals(keyProps.has_write_permission) ? apiKey.writeKey : null;
            projects.computeIfAbsent(apiKey.project, (k) -> Lists.newArrayList())
                    .add(new ProjectApiKeys(apiKey.id, apiKey.project, masterKey, readKey, writeKey));
        }
        return projects;
    }


    public void revokeApiKeys(int user, String project, int id) {
        if (!metastore.getProjects().contains(project)) {
            throw new RakamException("Project does not exists", BAD_REQUEST);
        }

        try (Handle handle = dbi.open()) {
            if (!getUserApiKeys(handle, user).stream().anyMatch(a -> a.project.equals(project))) {
                // TODO: check scope permission keys
                throw new RakamException(UNAUTHORIZED);
            }

            apiKeyService.revokeApiKeys(project, id);

            handle.createStatement("DELETE FROM web_user_project " +
                    "WHERE id = :id AND project = :project AND user_id = :user_id")
                    .bind("id", id)
                    .bind("user_id", user)
                    .bind("project", project).execute();
        }
    }
}
