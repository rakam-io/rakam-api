package org.rakam;

import org.rakam.ServiceStarter.ConfigInspectorModule.ConfigItem;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.util.List;

@Path("/admin")
@Api(value = "/admin", description = "System operations", tags = "admin")
public class AdminHttpService extends HttpService {
    private final SystemRegistry systemRegistry;
    private final List<ConfigItem> configItems;

    @Inject
    public AdminHttpService(SystemRegistry systemRegistry, List<ConfigItem> configItems) {
        this.systemRegistry = systemRegistry;
        this.configItems = configItems;
    }


    @ApiOperation(value = "List installed modules",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @GET
    @Path("/modules")
    public List<SystemRegistry.Module> getModules() {
        return systemRegistry.getModules();
    }


    @ApiOperation(value = "List configurations",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @GET
    @Path("/configs")
    public List<ConfigItem> getConfiguration() {
        return configItems;
    }
}
