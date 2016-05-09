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
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.user.WebUserHttpService;
import org.rakam.util.JsonHelper;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.rakam.util.JsonHelper.encode;


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
                                   @javax.inject.Named("project") String project) {
        return metadata.list(reportType, project);
    }

    @GET
    @Path("/types")
    @ApiOperation(value = "List report types", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public List<String> types(@javax.inject.Named("project") String project) {
        return metadata.types(project);
    }

    @ApiOperation(value = "Create reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"),
            response = JsonResponse.class, request = CustomReport.class)
    @Path("/create")
    public void create(@javax.inject.Named("project") String project, RakamHttpRequest request) {
        request.bodyHandler(body -> {
            CustomReport report = JsonHelper.read(body, CustomReport.class);

            Optional<Integer> user = request.cookies().stream().filter(a -> a.name().equals("session")).findFirst()
                    .map(cookie -> WebUserHttpService.extractUserFromCookie(cookie.value(), encryptionConfig.getSecretKey()));

            if (!user.isPresent()) {
                request.response(encode(JsonResponse.error("Unauthorized")), UNAUTHORIZED).end();
            } else {
                metadata.save(user.get(), project, report);
                request.response(encode(JsonResponse.success()), OK).end();
            }
        });
    }

    @JsonRequest
    @Path("/update")
    @ApiOperation(value = "Update reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public JsonResponse update(@javax.inject.Named("project") String project, @BodyParam CustomReport report) {
        metadata.update(project, report);
        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/delete")
    @ApiOperation(value = "Delete reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public JsonResponse delete(@javax.inject.Named("project") String project,
                               @ApiParam("report_type") String reportType,
                               @ApiParam("name") String name) {
        metadata.delete(reportType, project, name);

        return JsonResponse.success();
    }

    @JsonRequest
    @Path("/get")
    @ApiOperation(value = "Get reports", tags = "rakam-ui", authorizations = @Authorization(value = "read_key"))
    public Object get(@ApiParam("report_type") String reportType,
                      @javax.inject.Named("project") String project,
                      @ApiParam(value = "name") String name) {
        return metadata.get(reportType, project, name);
    }
}
