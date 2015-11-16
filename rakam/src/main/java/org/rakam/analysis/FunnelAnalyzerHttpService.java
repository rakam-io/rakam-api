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
package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.report.QueryHttpService;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.rakam.util.ValidationUtil.checkTableColumn;

@Path("/funnel")
@Api(value = "/funnel", tags = "funnel")
public class FunnelAnalyzerHttpService extends HttpService {
    private final FunnelQueryExecutor funnelQueryExecutor;
    private final QueryHttpService queryService;

    @Inject
    public FunnelAnalyzerHttpService(FunnelQueryExecutor funnelQueryExecutor, QueryHttpService queryService) {
        this.funnelQueryExecutor = funnelQueryExecutor;
        this.queryService = queryService;
    }

    @ApiOperation(value = "Execute query",
            request = FunnelQuery.class,
            authorizations = @Authorization(value = "read_key")
    )
    @GET
    @Path("/analyze")
    public void analyze(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, FunnelQuery.class, (query) ->
                funnelQueryExecutor.query(query.project,
                        query.connectorField,
                        query.steps,
                        Optional.ofNullable(query.dimension),
                        query.startDate,
                        query.endDate, query.enableOtherGrouping));
    }

    private static class FunnelQuery {
        public final String project;
        public final String connectorField;
        public final List<FunnelQueryExecutor.FunnelStep> steps;
        public final String dimension;
        public final LocalDate startDate;
        public final LocalDate endDate;
        public final boolean enableOtherGrouping;

        @JsonCreator
        public FunnelQuery(@ApiParam(name="project") String project,
                           @ApiParam(name="connector_field") String connectorField,
                           @ApiParam(name="steps") List<FunnelQueryExecutor.FunnelStep> steps,
                           @ApiParam(name="dimension", required = false) String dimension,
                           @ApiParam(name="startDate") LocalDate startDate,
                           @ApiParam(name="endDate") LocalDate endDate,
                           @ApiParam(name="enableOtherGrouping", required = false) Boolean enableOtherGrouping) {
            this.project = project;
            this.connectorField = checkTableColumn(connectorField, "connector field");
            this.enableOtherGrouping = enableOtherGrouping == null ? false : enableOtherGrouping.booleanValue();
            this.steps = checkNotNull(steps, "steps field is required");
            this.dimension = dimension;
            this.startDate = startDate;
            this.endDate = endDate;
            checkState(steps.size() > 0, "steps field cannot be empty.");
        }
    }
}
