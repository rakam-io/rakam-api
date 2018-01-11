package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.UIPermissionParameterProvider.Project;
import org.rakam.ui.user.WebUser;
import org.rakam.ui.user.WebUserService;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.StringMapper;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static java.lang.String.format;

@Path("/ui/cluster")
@IgnoreApi
public class ClusterService
        extends HttpService {
    private final DBI dbi;
    private final WebUserService webUserService;

    @Inject
    public ClusterService(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource,
                          WebUserService webUserService) {
        dbi = new DBI(dataSource);
        this.webUserService = webUserService;
    }

    @JsonRequest
    @ProtectEndpoint(requiresProject = false)
    @ApiOperation(value = "Register cluster", authorizations = @Authorization(value = "read_key"))
    @Path("/register")
    public SuccessMessage register(@javax.inject.Named("user_id") Project project,
                                   @BodyParam Cluster cluster) {
        Optional<WebUser> webUser = webUserService.getUser(project.userId);
        if (webUser.get().readOnly) {
            throw new RakamException("User is not allowed to register clusters", UNAUTHORIZED);
        }

        if (!cluster.apiUrl.getPath().isEmpty() && !cluster.apiUrl.getPath().equals("/")) {
            throw new RakamException(format("The API URL must not include path '%s'", cluster.apiUrl.getPath()),
                    BAD_REQUEST);
        }

        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO rakam_cluster (user_id, api_url, lock_key) VALUES (:userId, :apiUrl, :lockKey)")
                        .bind("userId", project.userId)
                        .bind("apiUrl", cluster.apiUrl.toString())
                        .bind("lockKey", cluster.lockKey).execute();
            } catch (Throwable e) {
                int execute = handle.createStatement("UPDATE rakam_cluster SET lock_key = :lock_key WHERE user_id = :userId AND api_url = :apiUrl")
                        .bind("userId", project.userId)
                        .bind("apiUrl", cluster.apiUrl.toString())
                        .bind("lock_key", cluster.lockKey).execute();

                if (execute == 0) {
                    throw new IllegalStateException();
                }

                return SuccessMessage.success("Lock key is updated");
            }

            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ProtectEndpoint(requiresProject = false)
    @ApiOperation(value = "Delete cluster", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public SuccessMessage delete(@javax.inject.Named("user_id") Project project,
                                 @ApiParam("api_url") String apiUrl) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM rakam_cluster WHERE (user_id, api_url) VALUES (:userId, :apiUrl)")
                    .bind("userId", project.userId)
                    .bind("apiUrl", apiUrl).execute();
            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ProtectEndpoint(requiresProject = false)
    @ApiOperation(value = "List cluster", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    @GET
    public List<String> list(@javax.inject.Named("user_id") Project project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT api_url FROM rakam_cluster WHERE user_id = :userId")
                    .bind("userId", project.userId).map(StringMapper.FIRST).list();
        }
    }

    public static class Cluster {
        public final URL apiUrl;
        public final String lockKey;

        @JsonCreator
        public Cluster(@ApiParam("api_url") URL apiUrl,
                       @ApiParam(value = "lock_key", required = false) String lockKey) {
            this.apiUrl = apiUrl;
            this.lockKey = lockKey;
        }
    }
}
