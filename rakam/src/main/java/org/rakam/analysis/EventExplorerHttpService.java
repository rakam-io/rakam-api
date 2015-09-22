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
import javax.inject.Inject;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;

import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/08/15 07:15.
 */
@Path("/event-explorer")
@Api(value = "/event-explorer", description = "Event explorer module", tags = "event")
public class EventExplorerHttpService extends HttpService {
    private final EventExplorer eventExplorer;

    @Inject
    public EventExplorerHttpService(EventExplorer eventExplorer) {
        this.eventExplorer = eventExplorer;
    }

    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @JsonRequest
    @Path("/statistics")
    public CompletableFuture<QueryResult> getEventStatistics(@ApiParam(name = "project", required = true) String project,
                                                             @ApiParam(name = "dimension") String dimension,
                                                             @ApiParam(name = "startDate", required = true) LocalDate startDate,
                                                             @ApiParam(name = "endDate", required = true) LocalDate endDate) {
        return eventExplorer.getEventStatistics(project, Optional.ofNullable(dimension), startDate, endDate);
    }


    @JsonRequest
    @Path("/extra_dimensions")
    public List<String> getExtraDimensions(@ApiParam(name = "project", required = true) String project) {
        return eventExplorer.getExtraDimensions(project);
    }

    @JsonRequest
    @Path("/event_dimensions")
    public List<String> getEventDimensions(@ApiParam(name = "project", required = true) String project) {
        return eventExplorer.getEventDimensions(project);
    }

    @ApiOperation(value = "Analyze event data-set",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> execute(@ApiParam(name = "project", required = true) String project,
                                                  @ApiParam(name = "measure", required = false) EventExplorer.Measure measure,
                                                  @ApiParam(name = "grouping", required = false) EventExplorer.Reference grouping,
                                                  @ApiParam(name = "segment", required = false) EventExplorer.Reference segment,
                                                  @ApiParam(name = "filterExpression", required = false) String filterExpression,
                                                  @ApiParam(name = "startDate", required = true) LocalDate startDate,
                                                  @ApiParam(name = "endDate", required = true) LocalDate endDate,
                                                  @ApiParam(name="collections", required = true) List<String> collections) {
        checkArgument(collections.size() > 0, "collections array is empty");
        checkArgument(!measure.column.equals("time"), "measure column value cannot be 'time'");
        return eventExplorer.analyze(project,
                                            collections,
                                            measure, grouping,
                                            segment, filterExpression,
                                            startDate, endDate);
    }

    public static class AnalyzeRequest {
        @ApiParam(name = "project", required = true)
        public final String project;
        @ApiParam(name = "measure", required = false)
        public final EventExplorer.Measure measure;
        @ApiParam(name = "grouping", required = false)
        public final EventExplorer.Reference grouping;
        @ApiParam(name = "segment", required = false)
        public final EventExplorer.Reference segment;
        @ApiParam(name = "filterExpression", required = false)
        public final String filterExpression;
        @ApiParam(name = "startDate", required = true)
        public final LocalDate startDate;
        @ApiParam(name = "endDate", required = true)
        public final LocalDate endDate;
        @ApiParam(name="collections", required = true)
        private final List<String> collections;

        @JsonCreator
        public AnalyzeRequest(@JsonProperty("project") String project,
                              @JsonProperty("collections") List<String> collections,
                              @JsonProperty("measure") EventExplorer.Measure measure,
                              @JsonProperty("grouping") EventExplorer.Reference grouping,
                              @JsonProperty("segment") EventExplorer.Reference segment,
                              @JsonProperty("filterExpression") String filterExpression,
                              @JsonProperty("startDate") LocalDate startDate,
                              @JsonProperty("endDate") LocalDate endDate) {
            checkState(collections.size() > 0, "collection field is empty");
            this.project = project;
            this.measure = measure;
            this.collections = collections;
            this.grouping = grouping;
            this.segment = segment;
            this.filterExpression = filterExpression;
            this.startDate = startDate;
            this.endDate = endDate;
        }
    }


}
