package org.rakam.collection;

import org.rakam.aws.lambda.AWSLambdaService;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.util.List;
import java.util.Optional;

@Path("/task")
@Api(value = "/task", nickname = "task", description = "Tasks for automatic event collection", tags = {"collect", "task"})
public class ScheduledTaskHttpService
        extends HttpService
{
    private final AWSLambdaService service;

    @Inject
    public ScheduledTaskHttpService(AWSLambdaService service)
    {
        this.service = service;
    }

    @GET
    @ApiOperation(value = "Get tasks from market", authorizations = @Authorization(value = "master_key"))
    @Path("/get-market")
    public AWSLambdaService.TaskList listTasksFromMarket(@Named("project") String project, @ApiParam("cursor") String cursor)
    {
        return service.listTasksFromMarket(Optional.ofNullable(cursor));
    }

    @GET
    @ApiOperation(value = "List tasks", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    public List<AWSLambdaService.Task> listTasks(@Named("project") String project)
    {
        return service.getActiveTasks();
    }
}
