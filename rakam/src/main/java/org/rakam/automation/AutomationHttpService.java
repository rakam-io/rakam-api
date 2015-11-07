package org.rakam.automation;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;

import javax.inject.Inject;
import javax.ws.rs.Path;
import java.util.List;

@Path("/automation")
@Api(value = "/automation", description = "Automation module", tags = "automation")
public class AutomationHttpService extends HttpService {

    private final UserAutomationService service;

    @Inject
    public AutomationHttpService(UserAutomationService service) {
        this.service = service;
    }

    @ApiOperation(value = "Add scenario",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/add")
    public void add(UserAutomationService.AutomationRule rule) {
        service.add(rule);
    }

    @ApiOperation(value = "Remove scenario",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/remove")
    public void remove(String project, int id) {
        service.remove(project, id);
    }

    @ApiOperation(value = "List scenarios",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/list")
    public List<UserAutomationService.AutomationRule> list(@ApiParam(name="project") String project) {
        return service.list(project);
    }
}
