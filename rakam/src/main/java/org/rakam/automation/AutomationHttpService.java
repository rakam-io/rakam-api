package org.rakam.automation;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;

@Path("/automation")
@Api(value = "/automation", nickname = "automation", description = "Automation module", tags = "automation")
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
    public SuccessMessage addRule(@Named("project") String project, @BodyParam AutomationRule rule) {
        service.add(project, rule);
        return SuccessMessage.success();
    }

    @ApiOperation(value = "Remove rule",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/remove")
    public SuccessMessage removeRule(@Named("project") String project, @ApiParam("id") int id) {
        service.remove(project, id);
        return SuccessMessage.success();
    }

    @ApiOperation(value = "Deactivate rule",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/deactivate")
    public SuccessMessage deactivateRule(@Named("project") String project, @ApiParam("id") int id) {
        service.deactivate(project, id);
        return SuccessMessage.success();
    }

    @ApiOperation(value = "Activate rule",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/activate")
    public SuccessMessage activateRule(@Named("project") String project, @ApiParam("id") int id) {
        service.activate(project, id);
        return SuccessMessage.success();
    }

    @ApiOperation(value = "List scenarios",
            authorizations = @Authorization(value = "read_key")
    )
    @GET
    @Path("/list")
    public List<AutomationRule> listRules(@Named("project") String project) {
        return service.list(project);
    }
}
