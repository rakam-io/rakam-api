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
package org.rakam.analysis.eventexplorer;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.analysis.EventExplorer;
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
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.rakam.util.ValidationUtil.checkArgument;

@Path("/event-explorer")
@Api(value = "/event-explorer", nickname = "eventExplorer", description = "Event explorer module", tags = "event-explorer")
public class EventExplorerHttpService extends HttpService {
    private final EventExplorer eventExplorer;
    private final QueryHttpService queryService;

    @Inject
    public EventExplorerHttpService(EventExplorer eventExplorer, QueryHttpService queryService) {
        this.eventExplorer = eventExplorer;
        this.queryService = queryService;
    }

    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/statistics")
    public CompletableFuture<QueryResult> getEventStatistics(@ApiParam(name = "project") String project,
                                                             @ApiParam(name = "collections", required = false) Set<String> collections,
                                                             @ApiParam(name = "dimension", required = false) String dimension,
                                                             @ApiParam(name = "startDate") LocalDate startDate,
                                                             @ApiParam(name = "endDate") LocalDate endDate) {
        return eventExplorer.getEventStatistics(project, Optional.ofNullable(collections), Optional.ofNullable(dimension), startDate, endDate);
    }


    @JsonRequest
    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/extra_dimensions")
    public List<String> getExtraDimensions(@ApiParam(name = "project", required = true) String project) {
        return eventExplorer.getExtraDimensions(project);
    }

    @ApiOperation(value = "Perform simple query on event data",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> analyze(@ParamBody AnalyzeRequest analyzeRequest) {
        checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");
        checkArgument(!analyzeRequest.measure.column.equals("_time"), "measure column value cannot be '_time'");

        return eventExplorer.analyze(analyzeRequest.project, analyzeRequest.collections,
                analyzeRequest.measure, analyzeRequest.grouping,
                analyzeRequest.segment, analyzeRequest.filterExpression,
                analyzeRequest.startDate, analyzeRequest.endDate).getResult();
    }

    @ApiOperation(value = "Perform simple query on event data",
            request = AnalyzeRequest.class,
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key")
    )
    @GET
    @IgnoreApi
    @IgnorePermissionCheck
    @Path("/analyze")
    public void analyze(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, AnalyzeRequest.class, (analyzeRequest) -> {
            checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");
            checkArgument(!analyzeRequest.measure.column.equals("_time"), "measure column value cannot be '_time'");
            return eventExplorer.analyze(analyzeRequest.project, analyzeRequest.collections,
                    analyzeRequest.measure, analyzeRequest.grouping,
                    analyzeRequest.segment, analyzeRequest.filterExpression,
                    analyzeRequest.startDate, analyzeRequest.endDate);
        });
    }

    public static class AnalyzeRequest implements ProjectItem {
        public final String project;
        public final EventExplorer.Measure measure;
        public final EventExplorer.Reference grouping;
        public final EventExplorer.Reference segment;
        public final String filterExpression;
        public final LocalDate startDate;
        public final LocalDate endDate;
        public final List<String> collections;

        @JsonCreator
        public AnalyzeRequest(@ApiParam(name = "project") String project,
                              @ApiParam(name = "measure", required = false) EventExplorer.Measure measure,
                              @ApiParam(name = "grouping", required = false) EventExplorer.Reference grouping,
                              @ApiParam(name = "segment", required = false) EventExplorer.Reference segment,
                              @ApiParam(name = "filterExpression", required = false) String filterExpression,
                              @ApiParam(name = "startDate") LocalDate startDate,
                              @ApiParam(name = "endDate") LocalDate endDate,
                              @ApiParam(name = "collections") List<String> collections) {
            this.project = project;
            this.measure = measure;
            this.grouping = grouping;
            this.segment = segment;
            this.filterExpression = filterExpression;
            this.startDate = startDate;
            this.endDate = endDate;
            this.collections = collections;
        }

        @Override
        public String project() {
            return project;
        }
    }
}
