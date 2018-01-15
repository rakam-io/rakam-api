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
import org.rakam.analysis.EventExplorer.OLAPTable;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.analysis.RequestContext;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryResult;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.rakam.util.ValidationUtil.checkArgument;

@Path("/event-explorer")
@Api(value = "/event-explorer", nickname = "eventExplorer", description = "Event explorer module", tags = "event-explorer")
public class EventExplorerHttpService
        extends HttpService {
    private final EventExplorer eventExplorer;
    private final QueryHttpService queryService;
    private final MaterializedViewService materializedViewService;
    private final ProjectConfig projectConfig;

    @Inject
    public EventExplorerHttpService(
            EventExplorer eventExplorer,
            MaterializedViewService materializedViewService,
            ProjectConfig projectConfig,
            QueryHttpService queryService) {
        this.eventExplorer = eventExplorer;
        this.queryService = queryService;
        this.projectConfig = projectConfig;
        this.materializedViewService = materializedViewService;
    }

    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/statistics")
    public CompletableFuture<QueryResult> getEventStatistics(RakamHttpRequest request,
                                                             @Named("project") RequestContext context,
                                                             @ApiParam(value = "collections", required = false) Set<String> collections,
                                                             @ApiParam(value = "dimension", required = false) String dimension,
                                                             @ApiParam("startDate") LocalDate startDate,
                                                             @ApiParam("endDate") LocalDate endDate,
                                                             @ApiParam(value = "timezone", required = false) ZoneId timezone) {
        timezone = timezone == null ? ZoneOffset.UTC : timezone;
        CompletableFuture<QueryResult> eventStatistics = eventExplorer.getEventStatistics(context,
                Optional.ofNullable(collections),
                Optional.ofNullable(dimension),
                startDate, endDate,
                Optional.ofNullable(timezone).orElse(ZoneOffset.UTC));

        request.context().channel().closeFuture()
                .addListener(future -> {
                    try {
                        eventStatistics.complete(QueryResult.empty());
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                });

        return eventStatistics;
    }

    @GET
    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/extra_dimensions")
    @JsonRequest
    public Map<String, List<String>> getExtraDimensions(@Named("project") RequestContext context) {
        return eventExplorer.getExtraDimensions(context.project);
    }

    @ApiOperation(value = "Perform simple query on event data",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> analyzeEvents(@Named("project") RequestContext context, @QueryParam("read_key") String readKey, @BodyParam AnalyzeRequest analyzeRequest) {
        checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");

        return eventExplorer.analyze(context, analyzeRequest.collections,
                analyzeRequest.measure, analyzeRequest.grouping,
                analyzeRequest.segment, analyzeRequest.filterExpression,
                analyzeRequest.startDate, analyzeRequest.endDate,
                Optional.ofNullable(analyzeRequest.timezone).orElse(ZoneOffset.UTC)).getResult();
    }

    @ApiOperation(value = "Create Pre-computed table",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/pre_calculate")
    public CompletableFuture<AbstractEventExplorer.PrecalculatedTable> createPrecomputedTable(@Named("project") RequestContext context, @BodyParam OLAPTable table) {
        return eventExplorer.create(context, table);
    }

    @ApiOperation(value = "Perform simple query on event data",
            request = AnalyzeRequest.class,
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key")
    )

    @GET
    @IgnoreApi
    @Path("/analyze")
    public void analyzeEvents(RakamHttpRequest request, @QueryParam("read_key") String readKey) {
        queryService.handleServerSentQueryExecution(request, AnalyzeRequest.class, (project, analyzeRequest) -> {
            checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");

            ZoneId timezone = Optional.ofNullable(analyzeRequest.timezone).orElse(ZoneOffset.UTC);
            return eventExplorer.analyze(new RequestContext(project, readKey), analyzeRequest.collections,
                    analyzeRequest.measure, analyzeRequest.grouping,
                    analyzeRequest.segment, analyzeRequest.filterExpression,
                    analyzeRequest.startDate, analyzeRequest.endDate,
                    timezone);
        });
    }

    @ApiOperation(value = "Perform simple query on event data",
            request = AnalyzeRequest.class,
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key")
    )

    @GET
    @IgnoreApi
    @Path("/analyze/export")
    public void exportEvents(RakamHttpRequest request, @QueryParam("read_key") String readKey) {
        queryService.handleServerSentQueryExecution(request, AnalyzeRequest.class, (project, analyzeRequest) -> {
            checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");

            return eventExplorer.export(new RequestContext(project, readKey), analyzeRequest.collections,
                    analyzeRequest.measure, analyzeRequest.grouping,
                    analyzeRequest.segment, analyzeRequest.filterExpression,
                    analyzeRequest.startDate, analyzeRequest.endDate,
                    Optional.ofNullable(analyzeRequest.timezone).orElse(ZoneOffset.UTC));
        });
    }

    public static class AnalyzeRequest {
        public final EventExplorer.Measure measure;
        public final EventExplorer.Reference grouping;
        public final EventExplorer.Reference segment;
        public final String filterExpression;
        public final LocalDate startDate;
        public final LocalDate endDate;
        public final ZoneId timezone;
        public final List<String> collections;

        @JsonCreator
        public AnalyzeRequest(@ApiParam(value = "measure") EventExplorer.Measure measure,
                              @ApiParam(value = "grouping", required = false) EventExplorer.Reference grouping,
                              @ApiParam(value = "segment", required = false) EventExplorer.Reference segment,
                              @ApiParam(value = "filterExpression", required = false) String filterExpression,
                              @ApiParam("startDate") LocalDate startDate,
                              @ApiParam("endDate") LocalDate endDate,
                              @ApiParam(value = "timezone", required = false) ZoneId timezone,
                              @ApiParam("collections") List<String> collections) {
            this.measure = measure;
            this.grouping = grouping;
            this.segment = segment;
            this.filterExpression = filterExpression;
            this.startDate = startDate;
            this.endDate = endDate;
            this.timezone = timezone;
            this.collections = collections;
        }
    }
}
