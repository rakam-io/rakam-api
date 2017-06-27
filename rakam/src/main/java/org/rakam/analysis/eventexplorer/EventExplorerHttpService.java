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
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.EventExplorer.OLAPTable;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.QueryHttpService;
import org.rakam.config.ProjectConfig;
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

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.report.realtime.AggregationType.SUM;
import static org.rakam.util.ValidationUtil.checkArgument;
import static org.rakam.util.ValidationUtil.checkTableColumn;

@Path("/event-explorer")
@Api(value = "/event-explorer", nickname = "eventExplorer", description = "Event explorer module", tags = "event-explorer")
public class EventExplorerHttpService
        extends HttpService
{
    private final EventExplorer eventExplorer;
    private final QueryHttpService queryService;
    private final MaterializedViewService materializedViewService;
    private final ProjectConfig projectConfig;

    @Inject
    public EventExplorerHttpService(
            EventExplorer eventExplorer,
            MaterializedViewService materializedViewService,
            ProjectConfig projectConfig,
            QueryHttpService queryService)
    {
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
    public CompletableFuture<QueryResult> getEventStatistics(@Named("project") String project,
            @ApiParam(value = "collections", required = false) Set<String> collections,
            @ApiParam(value = "dimension", required = false) String dimension,
            @ApiParam("startDate") Instant startDate,
            @ApiParam("endDate") Instant endDate,
            @ApiParam(value = "timezone", required = false) ZoneId timezone)
    {
        timezone = timezone == null ? ZoneOffset.UTC : timezone;
        return eventExplorer.getEventStatistics(project,
                Optional.ofNullable(collections),
                Optional.ofNullable(dimension),
                startDate, endDate,
                Optional.ofNullable(timezone).orElse(ZoneOffset.UTC));
    }

    @GET
    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/extra_dimensions")
    @JsonRequest
    public Map<String, List<String>> getExtraDimensions(@Named("project") String project)
    {
        return eventExplorer.getExtraDimensions(project);
    }

    @ApiOperation(value = "Perform simple query on event data",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> analyzeEvents(@Named("project") String project, @BodyParam AnalyzeRequest analyzeRequest)
    {
        checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");
        checkArgument(Optional.ofNullable(analyzeRequest.measure).map(e -> e.column).map(e -> !e.equals(projectConfig.getTimeColumn())).orElse(true),
                "measure column value cannot be time column");

        return eventExplorer.analyze(project, analyzeRequest.collections,
                analyzeRequest.measure, analyzeRequest.grouping,
                analyzeRequest.segment, analyzeRequest.filterExpression,
                analyzeRequest.startDate, analyzeRequest.endDate,
                Optional.ofNullable(analyzeRequest.timezone).orElse(ZoneOffset.UTC)).getResult();
    }

    public static class PrecalculatedTable
    {
        public final String name;
        public final String tableName;

        @JsonCreator
        public PrecalculatedTable(@ApiParam("name") String name, @ApiParam("tableName") String tableName)
        {
            this.name = name;
            this.tableName = tableName;
        }
    }

    @ApiOperation(value = "Create Pre-computed table",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/pre_calculate")
    public CompletableFuture<PrecalculatedTable> createPrecomputedTable(@Named("project") String project, @BodyParam OLAPTable table)
    {
        String dimensions = table.dimensions.stream().collect(Collectors.joining(", "));

        String subQuery;
        String measures = table.measures.isEmpty() ? "" : (", " + table.measures.stream().collect(Collectors.joining(", ")));
        String dimension = dimensions.isEmpty() ? "" : ", " + dimensions;

        if (table.collections != null && !table.collections.isEmpty()) {
            subQuery = table.collections.stream().map(collection ->
                    format("SELECT cast('%s' as varchar) as _collection, %s %s %s FROM %s",
                            collection,
                            checkTableColumn(projectConfig.getTimeColumn()),
                            dimension,
                            measures, collection))
                    .collect(Collectors.joining(" UNION ALL "));
        }
        else {
            subQuery = format("SELECT _collection, %s %s FROM _all",
                    checkTableColumn(projectConfig.getTimeColumn()), dimension, measures);
        }

        String name = "Dimensions";

        String metrics = table.measures.stream().map(column -> table.aggregations.stream()
                .map(agg -> getAggregationColumn(agg, table.aggregations).map(e -> format(e, column) + " as " + column + "_" + agg.name().toLowerCase()))
                .filter(Optional::isPresent).map(Optional::get).collect(Collectors.joining(", ")))
                .collect(Collectors.joining(", "));
        if (metrics.isEmpty()) {
            metrics = "count(*)";
        }

        String query = format("SELECT _collection, _time %s %s FROM " +
                        "(SELECT _collection, CAST(%s AS DATE) as _time %s %s FROM (%s) data) data " +
                        "GROUP BY CUBE (_collection, _time %s) ORDER BY 1 ASC",
                dimension,
                ", " + metrics,
                checkTableColumn(projectConfig.getTimeColumn()),
                dimension,
                measures,
                subQuery,
                dimensions.isEmpty() ? "" : "," + dimensions);

        return materializedViewService.create(project, new MaterializedView(table.tableName, "Olap table", query,
                Duration.ofHours(1), null, null, ImmutableMap.of("olap_table", table)))
                .thenApply(v -> new PrecalculatedTable(name, table.tableName));
    }

    private Optional<String> getAggregationColumn(AggregationType agg, Set<AggregationType> aggregations)
    {
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
    public void analyzeEvents(RakamHttpRequest request)
    {
        queryService.handleServerSentQueryExecution(request, AnalyzeRequest.class, (project, analyzeRequest) -> {
            checkArgument(!analyzeRequest.collections.isEmpty(), "collections array is empty");
            if (analyzeRequest.measure.column != null) {
                checkArgument(!analyzeRequest.measure.column.equals(projectConfig.getTimeColumn()), "measure column value cannot be time column");
            }

            return eventExplorer.analyze(project, analyzeRequest.collections,
                    analyzeRequest.measure, analyzeRequest.grouping,
                    analyzeRequest.segment, analyzeRequest.filterExpression,
                    analyzeRequest.startDate, analyzeRequest.endDate,
                    Optional.ofNullable(analyzeRequest.timezone).orElse(ZoneOffset.UTC));
        });
    }

    public static class AnalyzeRequest
    {
        public final EventExplorer.Measure measure;
        public final EventExplorer.Reference grouping;
        public final EventExplorer.Reference segment;
        public final String filterExpression;
        public final Instant startDate;
        public final Instant endDate;
        public final ZoneId timezone;
        public final List<String> collections;

        @JsonCreator
        public AnalyzeRequest(@ApiParam(value = "measure", required = false) EventExplorer.Measure measure,
                @ApiParam(value = "grouping", required = false) EventExplorer.Reference grouping,
                @ApiParam(value = "segment", required = false) EventExplorer.Reference segment,
                @ApiParam(value = "filterExpression", required = false) String filterExpression,
                @ApiParam("startDate") Instant startDate,
                @ApiParam("endDate") Instant endDate,
                @ApiParam(value = "timezone", required = false) ZoneId timezone,
                @ApiParam("collections") List<String> collections)
        {
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
