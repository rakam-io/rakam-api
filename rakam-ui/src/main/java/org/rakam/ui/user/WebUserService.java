package org.rakam.ui.user;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.lambdaworks.crypto.SCryptUtil;
import org.rakam.config.EncryptionConfig;
import org.rakam.ui.RakamUIConfig;
import org.rakam.util.AlreadyExistsException;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.Metastore.ProjectApiKeys;
import org.rakam.util.CryptUtil;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class WebUserService {

    private final DBI dbi;
    private final Metastore metastore;
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$");
    private static final Pattern PASSWORD_PATTERN = Pattern.compile("^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).{8,}$");
    private final RakamUIConfig config;
    private final EncryptionConfig encryptionConfig;

    @Inject
    public WebUserService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
                          Metastore metastore, RakamUIConfig config, EncryptionConfig encryptionConfig) {
        dbi = new DBI(dataSource);
        this.metastore = metastore;
        this.config = config;
        this.encryptionConfig = encryptionConfig;
        setup();
    }

    private void setup() {
        try (Handle handle = dbi.open()) {

            handle.createStatement("CREATE TABLE IF NOT EXISTS web_user (" +
                    "  id SERIAL PRIMARY KEY,\n" +
                    "  email TEXT NOT NULL UNIQUE,\n" +
                    "  is_activated BOOLEAN DEFAULT false NOT NULL,\n" +
                    "  password TEXT NOT NULL,\n" +
                    "  name TEXT NOT NULL,\n" +
                    "  created_at TIMESTAMP DEFAULT current_timestamp NOT NULL\n" +
                    "  )")
                    .execute();

            handle.createStatement("CREATE TABLE IF NOT EXISTS web_user_project (" +
                    "  id SERIAL PRIMARY KEY,\n" +
                    "  user_id INTEGER REFERENCES web_user(id),\n" +
                    "  project TEXT NOT NULL,\n" +
                    "  scope_expression TEXT,\n" +
                    "  has_read_permission BOOLEAN NOT NULL,\n" +
                    "  has_write_permission BOOLEAN NOT NULL,\n" +
                    "  is_admin BOOLEAN DEFAULT false NOT NULL\n" +
                    "  )")
                    .execute();
        }
    }

    public WebUser createUser(String email, String password, String name) {
        if (!PASSWORD_PATTERN.matcher(password).matches()) {
            throw new RakamException("Password is not valid. Your password must contain at least one lowercase character, uppercase character and digit and be at least 8 characters. ", BAD_REQUEST);
        }

        if(config.getHashPassword()) {
            password = CryptUtil.encryptWithHMacSHA1(password, encryptionConfig.getSecretKey());
        }
        final String scrypt = SCryptUtil.scrypt(password, 2 << 14, 8, 1);

        if (!EMAIL_PATTERN.matcher(email).matches()) {
            throw new RakamException("Email is not valid", BAD_REQUEST);
        }

        try (Handle handle = dbi.open()) {
            try {
                int id = handle.createStatement("INSERT INTO web_user (email, password, name) VALUES (:email, :password, :name)")
                        .bind("email", email)
                        .bind("name", name)
                        .bind("password", scrypt).executeAndReturnGeneratedKeys(IntegerMapper.FIRST).first();
                return new WebUser(id, email, name, ImmutableMap.of());
            } catch (UnableToExecuteStatementException e) {
                if (handle.createQuery("SELECT 1 FROM web_user WHERE email = :email").bind("email", email).first() != null) {
                    throw new AlreadyExistsException("A user with same email address", EXPECTATION_FAILED);
                }
                throw e;
            }
        }
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

        if(config.getHashPassword()) {
            oldPassword = CryptUtil.encryptWithHMacSHA1(oldPassword, encryptionConfig.getSecretKey());
        }

        try (Handle handle = dbi.open()) {
            String hashedPass = handle.createQuery("SELECT password FROM web_user WHERE id = :id")
                    .bind("id", id).map(StringMapper.FIRST).first();
            if (hashedPass == null) {
                throw new RakamException("User does not exist", BAD_REQUEST);
            }
            if(!SCryptUtil.check(oldPassword, hashedPass)) {
                throw new RakamException("Password is wrong", BAD_REQUEST);
            }
            handle.createStatement("UPDATE web_user SET password = :password WHERE id = :id AND password = :password")
                    .bind("id", id)
                    .bind("password", scrypt).execute();
        }
    }

    public WebUser.UserApiKey createProject(int user, String project) {
        if (metastore.getProjects().contains(project)) {
            throw new RakamException("Project already exists", BAD_REQUEST);
        }
        metastore.createProject(project);
        final ProjectApiKeys apiKeys = metastore.createApiKeys(project);
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
            handle.createQuery("SELECT web_user.email, project, scope_expression, has_read_permission, has_write_permission, is_admin FROM web_user_project " +
                    "JOIN web_user ON (web_user.id = web_user_project.user_id) WHERE project IN " +
                    "(SELECT DISTINCT project FROM web_user_project WHERE user_id = :user AND is_admin)")
                    .bind("user", user).map((i, resultSet, statementContext) -> {
                        return new SimpleImmutableEntry<>(resultSet.getString(2),
                                new UserAccess(resultSet.getString(1), resultSet.getString(3),
                                        resultSet.getBoolean(3), resultSet.getBoolean(4), resultSet.getBoolean(5)));
                    }).list().forEach(item -> map.computeIfAbsent(item.getKey(), (key) -> new ArrayList<>()).add(item.getValue()));
            return map;
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

            final ProjectApiKeys apiKeys = metastore.createApiKeys(project);

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

        if(config.getHashPassword()) {
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

        final List<ProjectApiKeys> apiKeys = metastore
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

            metastore.revokeApiKeys(project, id);

            handle.createStatement("DELETE FROM web_user_project " +
                    "WHERE id = :id AND project = :project AND user_id = :user_id")
                    .bind("id", id)
                    .bind("user_id", user)
                    .bind("project", project).execute();
        }
    }
}
