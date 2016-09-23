package org.rakam.analysis;

import org.rakam.bootstrap.SystemRegistry;
import org.rakam.bootstrap.SystemRegistry.ModuleDescriptor;
import org.rakam.collection.FieldType;
import org.rakam.config.ProjectConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.ActiveModuleListBuilder;
import org.rakam.ui.ActiveModuleListBuilder.ActiveModuleList;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Path("/admin")
@Api(value = "/admin", nickname = "admin", description = "System operations", tags = "admin")
public class AdminHttpService extends HttpService {
    private final SystemRegistry systemRegistry;
    private final ActiveModuleList activeModules;
    private final ProjectConfig projectConfig;

    @Inject
    public AdminHttpService(SystemRegistry systemRegistry, ProjectConfig projectConfig, ActiveModuleListBuilder activeModuleListBuilder) {
        this.systemRegistry = systemRegistry;
        this.projectConfig = projectConfig;
        activeModules = activeModuleListBuilder.build();
    }

    @ApiOperation(value = "List installed modules",
            authorizations = @Authorization(value = "master_key")
    )
    @GET
    @Path("/configurations")
    @JsonRequest
    public List<ModuleDescriptor> getConfigurations() {
        return systemRegistry.getModules();
    }

    @ApiOperation(value = "Get types",
            authorizations = @Authorization(value = "master_key")
    )

    @GET
    @JsonRequest
    @Path("/types")
    public Map<String, String> getTypes() {
        return Arrays.stream(FieldType.values()).collect(Collectors.toMap(FieldType::name, FieldType::getPrettyName));
    }

    @ApiOperation(value = "Check lock key",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/lock_key")
    public boolean checkLockKey(@ApiParam(value = "lock_key", required = false) String lockKey) {
        return Objects.equals(lockKey, projectConfig.getLockKey());
    }

    @Path("/modules")
    @GET
    @IgnoreApi
    @ApiOperation(value = "List installed modules for Rakam UI",
            authorizations = @Authorization(value = "master_key")
    )
    public ActiveModuleList modules() {
        return activeModules;
    }


}
