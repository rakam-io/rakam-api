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
package org.rakam.analysis.funnel;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.QueryHttpService;
import org.rakam.plugin.ProjectItem;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.server.http.annotations.ParamBody;
import org.rakam.util.IgnorePermissionCheck;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Path("/funnel")
@Api(value = "/funnel", nickname = "funnelAnalyzer", tags = "funnel")
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
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key")
    )
    @GET
    @IgnoreApi
    @IgnorePermissionCheck
    @Path("/analyze")
    public void analyze(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, FunnelQuery.class, (query) ->
                funnelQueryExecutor.query(query.project,
                        query.steps,
                        Optional.ofNullable(query.dimension),
                        query.startDate,
                        query.endDate, query.enableOtherGrouping));
    }

    @ApiOperation(value = "Execute query",
            request = FunnelQuery.class,
            authorizations = @Authorization(value = "read_key")
    )
    @POST
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> analyze(@ParamBody FunnelQuery query) {
         return funnelQueryExecutor.query(query.project,
                        query.steps,
                        Optional.ofNullable(query.dimension),
                        query.startDate,
                        query.endDate, query.enableOtherGrouping).getResult();
    }

    private static class FunnelQuery implements ProjectItem {
        public final String project;
        public final List<FunnelQueryExecutor.FunnelStep> steps;
        public final String dimension;
        public final LocalDate startDate;
        public final LocalDate endDate;
        public final boolean enableOtherGrouping;

        @JsonCreator
        public FunnelQuery(@ApiParam(name="project") String project,
                           @ApiParam(name="steps") List<FunnelQueryExecutor.FunnelStep> steps,
                           @ApiParam(name="dimension", required = false) String dimension,
                           @ApiParam(name="startDate") LocalDate startDate,
                           @ApiParam(name="endDate") LocalDate endDate,
                           @ApiParam(name="enableOtherGrouping", required = false) Boolean enableOtherGrouping) {
            this.project = project;
            this.enableOtherGrouping = enableOtherGrouping == null ? false : enableOtherGrouping.booleanValue();
            this.steps = checkNotNull(steps, "steps field is required");
            this.dimension = dimension;
            this.startDate = startDate;
            this.endDate = endDate;
            checkState(!steps.isEmpty(), "steps field cannot be empty.");
        }

        @Override
        public String project() {
            return project;
        }
    }
}
