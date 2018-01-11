package org.rakam.ui;

import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.analysis.CustomParameter;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpServerBuilder;
import org.rakam.server.http.IRequestParameter;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.util.RakamException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.BooleanMapper;

import javax.inject.Inject;
import java.lang.reflect.Method;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

public class UIPermissionParameterProvider
        implements Provider<CustomParameter> {

    private final DBI dbi;
    private final EncryptionConfig encryptionConfig;
    private final AuthService authService;

    @Inject
    public UIPermissionParameterProvider(@Named("ui.metadata.jdbc")
                                                 JDBCPoolDataSource dataSource,
                                         com.google.common.base.Optional<AuthService> authService, EncryptionConfig encryptionConfig) {
        this.dbi = new DBI(dataSource);
        this.encryptionConfig = encryptionConfig;
        this.authService = authService.orNull();
    }

    @Override
    public CustomParameter get() {
        return new CustomParameter("user_id", new Factory(authService, encryptionConfig, dbi));
    }

    public static class Factory
            implements HttpServerBuilder.IRequestParameterFactory {

        private final EncryptionConfig encryptionConfig;
        private final DBI dbi;
        private final AuthService authService;

        public Factory(AuthService authService, EncryptionConfig encryptionConfig, DBI dbi) {
            this.encryptionConfig = encryptionConfig;
            this.authService = authService;
            this.dbi = dbi;
        }

        @Override
        public IRequestParameter<Project> create(Method method) {
            boolean readOnly = !method.isAnnotationPresent(ProtectEndpoint.class) ||
                    !method.getAnnotation(ProtectEndpoint.class).writeOperation();
            boolean requiresProject = !method.isAnnotationPresent(ProtectEndpoint.class) ||
                    method.getAnnotation(ProtectEndpoint.class).requiresProject();

            return (objectNode, request) -> {
                String projectString = request.headers().get("project");
                if (projectString == null && requiresProject) {
                    throw new RakamException("Project header is null", BAD_REQUEST);
                }

                Optional<Cookie> session = request.cookies().stream().filter(e -> e.name().equals("session")).findAny();
                int userId = WebUserHttpService.extractUserFromCookie(session.orElseThrow(() -> new RakamException(UNAUTHORIZED)).value(),
                        encryptionConfig.getSecretKey());

                if (authService != null) {
                    authService.checkAccess(userId);
                }

                int project;
                if (projectString != null) {
                    try {
                        project = Integer.parseInt(projectString);
                    } catch (NumberFormatException e) {
                        throw new RakamException("Project header must be numeric", BAD_REQUEST);
                    }

                    if (readOnly) {
                        try (Handle handle = dbi.open()) {
                            boolean hasPermission = handle.createQuery("SELECT true FROM web_user_api_key key " +
                                    "JOIN web_user_project project ON (key.project_id = project.id) " +
                                    "WHERE key.user_id = :user AND project.id = :id " +
                                    " UNION ALL " +
                                    " SELECT true " +
                                    "FROM web_user_api_key_permission permission \n" +
                                    "JOIN web_user_api_key api_key ON (permission.api_key_id = api_key.id) \n" +
                                    "WHERE permission.user_id = :user AND api_key.project_id = :id AND (permission.read_permission or permission.master_permission)")
                                    .map(BooleanMapper.FIRST)
                                    .bind("user", userId)
                                    .bind("id", project)
                                    .first() != null;
                            if (!hasPermission) {
                                throw new RakamException(HttpResponseStatus.FORBIDDEN);
                            }
                        }
                    } else {
                        try (Handle handle = dbi.open()) {
                            Boolean readOnlyUser = handle.createQuery("SELECT web_user.read_only FROM web_user_api_key key " +
                                    "JOIN web_user_project project ON (key.project_id = project.id) " +
                                    "JOIN web_user ON (web_user.id = key.user_id) " +
                                    "WHERE key.user_id = :user AND project.id = :id" +
                                    " UNION ALL " +
                                    " SELECT web_user.read_only " +
                                    "FROM web_user_api_key_permission permission \n" +
                                    "JOIN web_user ON (web_user.id = permission.user_id) \n" +
                                    "JOIN web_user_api_key api_key ON (permission.api_key_id = api_key.id) \n" +
                                    "WHERE permission.user_id = :user AND api_key.project_id = :id AND (permission.master_permission OR permission.read_permission)")
                                    .map(BooleanMapper.FIRST)
                                    .bind("user", userId)
                                    .bind("id", project)
                                    .first();
                            if (readOnlyUser == null || readOnlyUser) {
                                throw new RakamException("The user read only and is not allowed to perform this operation", UNAUTHORIZED);
                            }
                        }
                    }
                } else {
                    project = -1;
                }

                return new Project(project, userId);
            };
        }
    }

    public static class Project {
        public final int project;
        public final int userId;

        public Project(int project, int userId) {
            this.project = project;
            this.userId = userId;
        }
    }
}
