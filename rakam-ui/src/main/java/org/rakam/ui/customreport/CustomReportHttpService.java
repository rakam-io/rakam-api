/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.ui.customreport;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.ProtectEndpoint;
import org.rakam.ui.UIPermissionParameterProvider.Project;
import org.rakam.ui.user.WebUserService;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@Path("/ui/custom-report")
@IgnoreApi
public class CustomReportHttpService
        extends HttpService {

    private final CustomReportMetadata metadata;
    private final WebUserService userService;

    @Inject
    public CustomReportHttpService(WebUserService userService, CustomReportMetadata metadata) {
        this.metadata = metadata;
        this.userService = userService;
    }

    @JsonRequest
    @Path("/list")
    @ApiOperation(value = "List reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public List<CustomReport> list(@ApiParam("report_type") String reportType,
                                   @Named("user_id") Project project) {
        return metadata.list(reportType, project.project);
    }

    @GET
    @Path("/types")
    @JsonRequest
    @ApiOperation(value = "List report types", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public List<String> types(@Named("user_id") Project project) {
        return metadata.types(project.project);
    }

    @ApiOperation(value = "Create reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"),
            response = SuccessMessage.class, request = CustomReport.class)
    @JsonRequest
    @ProtectEndpoint(writeOperation = true)
    @Path("/create")
    public SuccessMessage create(@Named("user_id") Project project, @BodyParam CustomReport report) {
        metadata.save(project.userId, project.project, report);
        return SuccessMessage.success();
    }

    @JsonRequest
    @Path("/update")
    @ProtectEndpoint(writeOperation = true)
    @ApiOperation(value = "Update reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public SuccessMessage update(@Named("user_id") Project project, @BodyParam CustomReport report) {
        CustomReport customReport = metadata.get(report.reportType, project.project, report.name);
        if (customReport.getUser() != null && customReport.getUser() != project.userId) {
            int id = userService.getProjectOwner(project.project).id;
            if (id != project.userId) {
                throw new RakamException("The owner of the custom report can update the report", UNAUTHORIZED);
            }
        }
        metadata.update(project.project, report);
        return SuccessMessage.success();
    }

    @JsonRequest
    @Path("/delete")
    @ApiOperation(value = "Delete reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    @ProtectEndpoint(writeOperation = true)
    public SuccessMessage delete(@Named("user_id") Project project,
                                 @ApiParam("report_type") String reportType,
                                 @ApiParam("name") String name) {
        CustomReport customReport = metadata.get(reportType, project.project, name);
        if (customReport == null) {
            throw new RakamException(NOT_FOUND);
        }
        if (customReport.getUser() != null && customReport.getUser() != project.userId) {
            int id = userService.getProjectOwner(project.project).id;
            if (id != project.userId) {
                throw new RakamException("The owner of the custom report can delete the report", UNAUTHORIZED);
            }
        }

        metadata.delete(reportType, project.project, name);
        return SuccessMessage.success();
    }

    @JsonRequest
    @Path("/get")
    @ApiOperation(value = "Get reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public CustomReport get(@ApiParam("report_type") String reportType,
                            @Named("user_id") Project project,
                            @ApiParam(value = "name") String name) {
        return metadata.get(reportType, project.project, name);
    }
}
