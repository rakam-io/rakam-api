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

import org.rakam.config.EncryptionConfig;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.CookieParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;

import static org.rakam.ui.user.WebUserHttpService.extractUserFromCookie;


@Path("/custom-report")
@IgnoreApi
public class CustomReportHttpService extends HttpService {

    private final JDBCCustomReportMetadata metadata;
    private final EncryptionConfig encryptionConfig;

    @Inject
    public CustomReportHttpService(JDBCCustomReportMetadata metadata, EncryptionConfig encryptionConfig) {
        this.metadata = metadata;
        this.encryptionConfig = encryptionConfig;
    }

    @JsonRequest
    @Path("/list")
    @ApiOperation(value = "List reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public List<CustomReport> list(@ApiParam("report_type") String reportType,
                                   @Named("project") String project) {
        return metadata.list(reportType, project);
    }

    @GET
    @Path("/types")
    @JsonRequest
    @ApiOperation(value = "List report types", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public List<String> types(@Named("project") String project) {
        return metadata.types(project);
    }


    @ApiOperation(value = "Create reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"),
            response = JsonResponse.class, request = CustomReport.class)
    @JsonRequest
    @Path("/create")
    public JsonResponse create(@Named("project") String project, @CookieParam(name = "session") String session, @BodyParam CustomReport report) {
        int userId = extractUserFromCookie(session, encryptionConfig.getSecretKey());

        metadata.save(userId, project, report);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/update")
    @ApiOperation(value = "Update reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public JsonResponse update(@Named("project") String project, @BodyParam CustomReport report) {
        metadata.update(project, report);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/delete")
    @ApiOperation(value = "Delete reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public JsonResponse delete(@Named("project") String project,
                               @ApiParam("report_type") String reportType,
                               @ApiParam("name") String name) {
        metadata.delete(reportType, project, name);

        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/get")
    @ApiOperation(value = "Get reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public Object get(@ApiParam("report_type") String reportType,
                      @Named("project") String project,
                      @ApiParam(value = "name") String name) {
        return metadata.get(reportType, project, name);
    }
}
