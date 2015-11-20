package org.rakam.automation;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonResponse;

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
    public JsonResponse addRule(@ParamBody AutomationRule rule) {
        service.add(rule);
        return JsonResponse.success();
    }

    @ApiOperation(value = "Remove rule",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/remove")
    public JsonResponse removeRule(@ApiParam(name = "project") String project, @ApiParam(name = "id") int id) {
        service.remove(project, id);
        return JsonResponse.success();
    }

    @ApiOperation(value = "Deactivate rule",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/deactivate")
    public JsonResponse deactivateRule(@ApiParam(name = "project") String project, @ApiParam(name = "id") int id) {
        service.deactivate(project, id);
        return JsonResponse.success();
    }

    @ApiOperation(value = "Activate rule",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/activate")
    public JsonResponse activateRule(@ApiParam(name = "project") String project, @ApiParam(name = "id") int id) {
        service.activate(project, id);
        return JsonResponse.success();
    }

    @ApiOperation(value = "List scenarios",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/list")
    public List<AutomationRule> listRules(@ApiParam(name="project") String project) {
        return service.list(project);
    }
}
