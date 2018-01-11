package org.rakam.ui.report;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.ProtectEndpoint;
import org.rakam.ui.ReportMetadata;
import org.rakam.ui.UIPermissionParameterProvider.Project;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

@Path("/ui/report")
@Api(value = "/report", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
@IgnoreApi
public class ReportHttpService
        extends HttpService {
    private final ReportMetadata metadata;

    @Inject
    public ReportHttpService(ReportMetadata metadata) {
        this.metadata = metadata;
    }

    @JsonRequest
    @ApiOperation(value = "List Reports", authorizations = @Authorization(value = "read_key"))
    @Path("/list")
    public Object list(@Named("user_id") Project project) {
        return metadata.list(project.userId, project.project);
    }

    @ApiOperation(value = "Create Report", authorizations = @Authorization(value = "read_key"))
    @Path("/create")
    @POST
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage create(@Named("user_id") Project project, @BodyParam Report report) {
        metadata.save(project.userId, project.project, report);
        return SuccessMessage.success();
    }

    @JsonRequest
    @ApiOperation(value = "Delete Report", authorizations = @Authorization(value = "read_key"))
    @Path("/delete")
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage delete(
            @Named("user_id") Project project,
            @ApiParam(value = "slug", description = "Slug") String slug) {
        metadata.delete(project.userId, project.project, slug);

        return SuccessMessage.success();
    }

    @JsonRequest
    @ApiOperation(value = "Get Report", authorizations = @Authorization(value = "read_key"))
    @Path("/get")
    public Report get(
            @ApiParam(value = "slug", description = "Report name") String slug,
            @Named("user_id") Project project) {
        return metadata.get(project.userId, project.project, slug);
    }

    @ApiOperation(value = "Update report", authorizations = @Authorization(value = "read_key"))
    @POST
    @Path("/update")
    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage update(@Named("user_id") Project project, @BodyParam Report report) {
        metadata.update(project.userId, project.project, report);
        return SuccessMessage.success();
    }
}
