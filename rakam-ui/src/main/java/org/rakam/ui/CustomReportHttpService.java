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
package org.rakam.ui;

import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.Path;
import java.util.List;


@Path("/custom-report")
@IgnoreApi
public class CustomReportHttpService extends HttpService {

    private final JDBCCustomReportMetadata metadata;

    @Inject
    public CustomReportHttpService(JDBCCustomReportMetadata metadata) {
        this.metadata = metadata;
    }

    @JsonRequest
    @Path("/list")
    @ApiOperation(value = "List reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public List<CustomReport> list(@ApiParam(name="report_type", required = true) String reportType,
                                   @ApiParam(name="project", required = true) String project) {
        return metadata.list(reportType, project);
    }

    @JsonRequest
    @ApiOperation(value = "Create reports", tags = "rakam-ui", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public JsonResponse create(@ParamBody CustomReport report) {
        metadata.add(report);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/update")
    @ApiOperation(value = "Update reports", tags = "rakam-ui", authorizations = @Authorization(value = "master_key"))
    public JsonResponse update(@ParamBody CustomReport report) {
        metadata.update(report);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/delete")
    @ApiOperation(value = "Delete reports", tags = "rakam-ui", authorizations = @Authorization(value = "master_key"))
    public JsonResponse delete(@ApiParam(name="report_type", required = true) String reportType,
                               @ApiParam(name="project", value = "Project id", required = true) String project,
                               @ApiParam(name="name", value = "Project name", required = true) String name) {
        metadata.delete(reportType, project, name);

        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/get")
    @ApiOperation(value = "Get reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public Object get(@ApiParam(name="report_type", required = true) String reportType,
                      @ApiParam(name="project", value = "Project id", required = true) String project,
                      @ApiParam(name="name", value = "Report name", required = true) String name) {
        return metadata.get(reportType, project, name);
    }
}
