package org.rakam;

import org.rakam.bootstrap.SystemRegistry;
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

    @Inject
    public AdminHttpService(SystemRegistry systemRegistry) {
        this.systemRegistry = systemRegistry;
    }


    @ApiOperation(value = "List installed modules",
            authorizations = @Authorization(value = "master_key")
    )
    @GET
    @Path("/modules")
    public List<SystemRegistry.ModuleDescriptor> getModules() {
        return systemRegistry.getModules();
    }
}
