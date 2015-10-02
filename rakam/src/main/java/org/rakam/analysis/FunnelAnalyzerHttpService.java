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
import com.fasterxml.jackson.annotation.JsonProperty;
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

@Path("/funnel")
@Api(value = "/funnel", description = "Funnel Analyzer module", tags = "event")
public class FunnelAnalyzerHttpService extends HttpService {
    private final FunnelQueryExecutor funnelQueryExecutor;
    private final QueryHttpService queryService;

    @Inject
    public FunnelAnalyzerHttpService(FunnelQueryExecutor funnelQueryExecutor, QueryHttpService queryService) {
        this.funnelQueryExecutor = funnelQueryExecutor;
        this.queryService = queryService;
    }

    @ApiOperation(value = "Analyze event data-set",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @GET
    @Path("/analyze")
    public void execute(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, FunnelQuery.class, (query) ->
                funnelQueryExecutor.query(query.project,
                        query.steps,
                        Optional.ofNullable(query.dimension),
                        query.startDate,
                        query.endDate, query.enableOtherGrouping));
    }

    private static class FunnelQuery {
        public final @ApiParam(name = "project", required = true) String project;
        public final @ApiParam(name = "steps", required = true) List<FunnelQueryExecutor.FunnelStep> steps;
        public final @ApiParam(name = "dimension", required = false) String dimension;
        public final @ApiParam(name = "startDate", required = true) LocalDate startDate;
        public final @ApiParam(name = "endDate", required = true) LocalDate endDate;
        public final @ApiParam(name = "enableOtherGrouping", required = false) boolean enableOtherGrouping;

        @JsonCreator
        private FunnelQuery(@JsonProperty("project") String project,
                            @JsonProperty("steps") List<FunnelQueryExecutor.FunnelStep> steps,
                            @JsonProperty("dimension") String dimension,
                            @JsonProperty("startDate") LocalDate startDate,
                            @JsonProperty("endDate") LocalDate endDate,
                            @JsonProperty("enableOtherGrouping") Boolean enableOtherGrouping) {
            this.project = project;
            this.enableOtherGrouping = enableOtherGrouping == null ? false : enableOtherGrouping.booleanValue();
            this.steps = checkNotNull(steps, "steps field is required");
            this.dimension = dimension;
            this.startDate = startDate;
            this.endDate = endDate;
            checkState(steps.size() > 0, "steps field cannot be empty.");
        }
    }
}
