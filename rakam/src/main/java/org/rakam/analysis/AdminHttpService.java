package org.rakam.analysis;

import org.rakam.bootstrap.SystemRegistry;
import org.rakam.collection.FieldType;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.Authorization;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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


    @ApiOperation(value = "Get types",
            authorizations = @Authorization(value = "master_key")
    )
    @GET
    @Path("/types")
    public Map<String, String> getTypes() {
        return Arrays.stream(FieldType.values()).collect(Collectors.toMap(FieldType::name, FieldType::getPrettyName));
    }
}
