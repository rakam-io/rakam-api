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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.FunnelQueryExecutor.FunnelStep;
import org.rakam.analysis.FunnelQueryExecutor.FunnelWindow;
import org.rakam.analysis.QueryHttpService;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.FieldType;
import org.rakam.config.ProjectConfig;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.rakam.analysis.FunnelQueryExecutor.FunnelType.*;

@Path("/funnel")
@Api(value = "/funnel", nickname = "funnelAnalyzer", tags = "funnel")
public class FunnelAnalyzerHttpService
        extends HttpService {
    private final static Logger LOGGER = Logger.get(FunnelAnalyzerHttpService.class);
    private final FunnelQueryExecutor funnelQueryExecutor;
    private final QueryHttpService queryService;
    private final ProjectConfig projectConfig;

    @Inject
    public FunnelAnalyzerHttpService(FunnelQueryExecutor funnelQueryExecutor, QueryHttpService queryService, ProjectConfig projectConfig) {
        this.funnelQueryExecutor = funnelQueryExecutor;
        this.queryService = queryService;
        this.projectConfig = projectConfig;
    }

    @ApiOperation(value = "Execute query",
            request = FunnelQuery.class,
            consumes = "text/event-stream",
            produces = "text/event-stream",
            authorizations = @Authorization(value = "read_key")
    )

    @GET
    @IgnoreApi
    @Path("/analyze")
    public void analyzeFunnel(RakamHttpRequest request, @QueryParam("read_key") String apiKey) {
        queryService.handleServerSentQueryExecution(request, FunnelQuery.class, (project, query) -> {
                    if (query.dimension != null && query.segment == null) {
                        if (projectConfig.getTimeColumn().equals(query.dimension)) {
                            throw new RakamException("You should use segments in order to group by with time column", BAD_REQUEST);
                        }
                    }

                    return funnelQueryExecutor.query(new RequestContext(project, apiKey),
                            query.steps,
                            Optional.ofNullable(query.dimension),
                            Optional.ofNullable(query.segment),
                            query.startDate,
                            query.endDate,
                            Optional.ofNullable(query.window),
                            query.timezone,
                            Optional.ofNullable(query.connectors),
                            getFunnelType(query));
                },
                (query, result) -> LOGGER.error(new RuntimeException(JsonHelper.encode(query) + " : " + result.getError().toString()), "Error running funnel query"));
    }

    private FunnelQueryExecutor.FunnelType getFunnelType(FunnelQuery query) {
        if (query.funnelType != null) {
            return query.funnelType;
        }

        if (Boolean.TRUE.equals(query.strictOrdering)) {
            return ORDERED;
        }

        if (Boolean.TRUE.equals(query.approximate)) {
            return APPROXIMATE;
        }

        return NORMAL;
    }

    @ApiOperation(value = "Execute query",
            request = FunnelQuery.class,
            authorizations = @Authorization(value = "read_key")
    )
    @POST
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> analyzeFunnel(@Named("project") RequestContext context, @BodyParam FunnelQuery query) {
        CompletableFuture<QueryResult> result = funnelQueryExecutor.query(context,
                query.steps,
                Optional.ofNullable(query.dimension),
                Optional.ofNullable(query.segment),
                query.startDate,
                query.endDate,
                Optional.ofNullable(query.window),
                query.timezone,
                Optional.ofNullable(query.connectors),
                getFunnelType(query)).getResult();
        result.thenAccept(data -> {
            if (data.isFailed()) {
                LOGGER.error(new RuntimeException(JsonHelper.encode(query) + " : " + data.getError().toString()),
                        "Error running funnel query");
            }
        });
        return result;
    }

    @GET
    @Path("/segments")
    public Map<FieldType, List<FunnelSegment>> segments() {
        return ImmutableMap.of(FieldType.TIMESTAMP,
                Arrays.stream(FunnelQueryExecutor.FunnelTimestampSegments.values()).map(e -> new FunnelSegment(e.name(), e.getDisplayName())).collect(Collectors.toList()));
    }

    public static class FunnelSegment {
        public final String value;
        public final String displayName;

        public FunnelSegment(String value, String displayName) {
            this.value = value;
            this.displayName = displayName;
        }
    }

    private static class FunnelQuery {
        public final List<FunnelStep> steps;
        public final String dimension;
        public final String segment;
        public final LocalDate startDate;
        public final FunnelWindow window;
        public final LocalDate endDate;
        public final ZoneId timezone;
        public final Boolean strictOrdering;
        public final List<String> connectors;
        public final Boolean approximate;
        public final FunnelQueryExecutor.FunnelType funnelType;

        @JsonCreator
        public FunnelQuery(@ApiParam("steps") List<FunnelStep> steps,
                           @ApiParam(value = "dimension", required = false) String dimension,
                           @ApiParam(value = "segment", required = false) String segment,
                           @ApiParam("startDate") LocalDate startDate,
                           @ApiParam(value = "window", required = false) FunnelWindow window,
                           @ApiParam("endDate") LocalDate endDate,
                           @ApiParam(value = "connectors", required = false) List<String> connectors,
                           @ApiParam(value = "strictOrdering", required = false) Boolean strictOrdering,
                           @ApiParam(value = "approximate", required = false) Boolean approximate,
                           @ApiParam(value = "funnelType", required = false) FunnelQueryExecutor.FunnelType funnelType,
                           @ApiParam(value = "timezone", required = false) String timezone) {
            this.steps = checkNotNull(steps, "steps field is required");
            this.dimension = dimension;
            this.segment = segment;
            this.startDate = startDate;
            this.endDate = endDate;
            this.strictOrdering = strictOrdering;
            this.connectors = connectors;
            this.window = window;
            this.approximate = approximate;
            this.funnelType = funnelType;
            try {
                this.timezone = Optional.ofNullable(timezone)
                        .map(t -> ZoneId.of(t))
                        .orElse(ZoneOffset.UTC);
            } catch (Exception e) {
                throw new RakamException("Timezone is invalid", BAD_REQUEST);
            }
            checkState(!steps.isEmpty(), "steps field cannot be empty.");
        }
    }
}
