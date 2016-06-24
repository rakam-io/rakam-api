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
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.EventExplorer.OLAPTable;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryResult;
import org.rakam.report.realtime.AggregationType;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.report.realtime.AggregationType.SUM;
import static org.rakam.util.ValidationUtil.checkArgument;

@Path("/event-explorer")
@Api(value = "/event-explorer", nickname = "eventExplorer", description = "Event explorer module", tags = "event-explorer")
public class EventExplorerHttpService extends HttpService {
    private final EventExplorer eventExplorer;
    private final QueryHttpService queryService;
    private final ContinuousQueryService continuousQueryService;
    private final MaterializedViewService materializedViewService;

    @Inject
    public EventExplorerHttpService(EventExplorer eventExplorer, ContinuousQueryService continuousQueryService,
                                    MaterializedViewService materializedViewService, QueryHttpService queryService) {
        this.eventExplorer = eventExplorer;
        this.queryService = queryService;
        this.continuousQueryService = continuousQueryService;
        this.materializedViewService = materializedViewService;
    }

    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/statistics")
    public CompletableFuture<QueryResult> getEventStatistics(@Named("project") String project,
                                                             @ApiParam(value = "collections", required = false) Set<String> collections,
                                                             @ApiParam(value = "dimension", required = false) String dimension,
                                                             @ApiParam("startDate") LocalDate startDate,
                                                             @ApiParam("endDate") LocalDate endDate) {
        return eventExplorer.getEventStatistics(project, Optional.ofNullable(collections), Optional.ofNullable(dimension), startDate, endDate);
    }


    @GET
    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/extra_dimensions")
    @JsonRequest
    public Map<String, List<String>> getExtraDimensions(@Named("project") String project) {
        return eventExplorer.getExtraDimensions(project);
    }

    @ApiOperation(value = "Perform simple query on event data",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> analyzeEvents(@Named("project") String project, @BodyParam AnalyzeRequest analyzeRequest) {
        checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");
        checkArgument(!analyzeRequest.measure.column.equals("_time"), "measure column value cannot be '_time'");

        return eventExplorer.analyze(project, analyzeRequest.collections,
                analyzeRequest.measure, analyzeRequest.grouping,
                analyzeRequest.segment, analyzeRequest.filterExpression,
                analyzeRequest.startDate, analyzeRequest.endDate).getResult();
    }

    public static class PrecalculatedTable {
        public final String name;
        public final String tableName;

        public PrecalculatedTable(String name, String tableName) {
            this.name = name;
            this.tableName = tableName;
        }
    }

    @ApiOperation(value = "Create Pre-computed table",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/pre_calculate")
    public CompletableFuture<PrecalculatedTable> createPrecomputedTable(@Named("project") String project, @BodyParam OLAPTable table) {
        String metrics = table.measures.stream().map(column -> table.aggregations.stream()
                .map(agg -> getAggregationColumn(agg, table.aggregations).map(e -> String.format(e, column) + " as " + column + "_" + agg.name().toLowerCase()))
                .filter(Optional::isPresent).map(Optional::get).collect(Collectors.joining(", ")))
                .collect(Collectors.joining(", "));

        String subQuery;
        String dimensions = table.dimensions.stream().collect(Collectors.joining(", "));
        if (table.collections.size() == 1) {
            subQuery = table.collections.iterator().next();
        } else if (table.collections.size() > 1) {
            subQuery = table.collections.stream().map(collection -> String.format("SELECT '%s' as collection, _time %s %s FROM %s",
                    collection,
                    dimensions.isEmpty() ? "" : ", " + dimensions,
                    table.measures.isEmpty() ? "" : ", " + table.measures.stream().collect(Collectors.joining(", ")), collection))
                    .collect(Collectors.joining(" UNION ALL "));
        } else {
            throw new RakamException("collections is empty", HttpResponseStatus.BAD_REQUEST);
        }

        String name = "Dimensions";

        String dimensionColumns = !dimensions.isEmpty() ? (dimensions + ",") : "";
        String collectionColumn = table.collections.size() != 1 ? ("collection,") : "";
        String query = String.format("SELECT %s _time, %s %s FROM (SELECT %s CAST(_time AS DATE) as _time, %s %s FROM (%s)) GROUP BY CUBE (_time %s %s) ORDER BY 1 ASC",
                collectionColumn, dimensionColumns, metrics,
                collectionColumn, dimensionColumns, table.measures.stream().collect(Collectors.joining(", ")),

                subQuery,
                table.collections.size() == 1 ? "" : ", collection", dimensions.isEmpty() ? "" : "," + dimensions);

        return materializedViewService.create(project, new MaterializedView(table.tableName, "Olap table", query,
                Duration.ofHours(1), null, ImmutableMap.of("olap_table", table)))
                .thenApply(v -> new PrecalculatedTable(name, table.tableName));
    }

    private Optional<String> getAggregationColumn(AggregationType agg, Set<AggregationType> aggregations) {
        switch (agg) {
            case AVERAGE:
                aggregations.add(COUNT);
                aggregations.add(SUM);
                return Optional.empty();
            case MAXIMUM:
                return Optional.of("max(%s)");
            case MINIMUM:
                return Optional.of("min(%s)");
            case COUNT:
                return Optional.of("count(%s)");
            case SUM:
                return Optional.of("sum(%s)");
            case COUNT_UNIQUE:
                throw new UnsupportedOperationException("Not supported yet.");
            case APPROXIMATE_UNIQUE:
                return Optional.of(eventExplorer.getIntermediateForApproximateUniqueFunction());
            default:
                throw new IllegalArgumentException("aggregation type is not supported");
        }
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
    public void analyzeEvents(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, AnalyzeRequest.class, (project, analyzeRequest) -> {
            checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");
            if(analyzeRequest.measure.column != null) {
                checkArgument(!analyzeRequest.measure.column.equals("_time"), "measure column value cannot be '_time'");
            }

            return eventExplorer.analyze(project, analyzeRequest.collections,
                    analyzeRequest.measure, analyzeRequest.grouping,
                    analyzeRequest.segment, analyzeRequest.filterExpression,
                    analyzeRequest.startDate, analyzeRequest.endDate);
        });
    }

    public static class AnalyzeRequest {
        public final EventExplorer.Measure measure;
        public final EventExplorer.Reference grouping;
        public final EventExplorer.Reference segment;
        public final String filterExpression;
        public final LocalDate startDate;
        public final LocalDate endDate;
        public final List<String> collections;

        @JsonCreator
        public AnalyzeRequest(@ApiParam(value = "measure", required = false) EventExplorer.Measure measure,
                              @ApiParam(value = "grouping", required = false) EventExplorer.Reference grouping,
                              @ApiParam(value = "segment", required = false) EventExplorer.Reference segment,
                              @ApiParam(value = "filterExpression", required = false) String filterExpression,
                              @ApiParam("startDate") LocalDate startDate,
                              @ApiParam("endDate") LocalDate endDate,
                              @ApiParam("collections") List<String> collections) {
            this.measure = measure;
            this.grouping = grouping;
            this.segment = segment;
            this.filterExpression = filterExpression;
            this.startDate = startDate;
            this.endDate = endDate;
            this.collections = collections;
        }
    }
}
