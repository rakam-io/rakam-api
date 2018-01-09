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
package org.rakam.analysis.retention;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.QueryHttpService;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.RetentionQueryExecutor.DateUnit;
import org.rakam.analysis.RetentionQueryExecutor.RetentionAction;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Path("/retention")
@Api(value = "/retention", nickname = "retentionAnalyzer", tags = "retention")
public class RetentionAnalyzerHttpService
        extends HttpService {
    private final static Logger LOGGER = Logger.get(RetentionAnalyzerHttpService.class);
    private final RetentionQueryExecutor retentionQueryExecutor;
    private final QueryHttpService queryService;

    @Inject
    public RetentionAnalyzerHttpService(RetentionQueryExecutor retentionQueryExecutor, QueryHttpService queryService) {
        this.retentionQueryExecutor = retentionQueryExecutor;
        this.queryService = queryService;
    }

    @ApiOperation(value = "Execute query",
            authorizations = @Authorization(value = "read_key"),
            consumes = "text/event-stream",
            produces = "text/event-stream"
    )
    @GET
    @IgnoreApi
    @Path("/analyze")
    public void analyzeRetention(RakamHttpRequest request, @QueryParam("read_key") String apiKey) {
        queryService.handleServerSentQueryExecution(request, RetentionQuery.class,
                (project, query) -> retentionQueryExecutor.query(new RequestContext(project, apiKey),
                Optional.ofNullable(query.firstAction),
                Optional.ofNullable(query.returningAction),
                query.dateUnit,
                Optional.ofNullable(query.dimension),
                Optional.ofNullable(query.period),
                query.startDate,
                query.endDate,
                query.timezone,
                query.approximate),
                (query, result) ->
                        LOGGER.error(new RuntimeException(JsonHelper.encode(query) + " : " + result.getError().toString()), "Error running retention query"));
    }

    @ApiOperation(value = "Execute query",
            authorizations = @Authorization(value = "read_key")
    )
    @POST
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> analyzeRetention(@Named("project") RequestContext context, @BodyParam RetentionQuery query) {
        CompletableFuture<QueryResult> result = retentionQueryExecutor.query(context,
                Optional.ofNullable(query.firstAction),
                Optional.ofNullable(query.returningAction),
                query.dateUnit,
                Optional.ofNullable(query.dimension),
                Optional.ofNullable(query.period),
                query.startDate,
                query.endDate,
                query.timezone,
                query.approximate).getResult();
        result.thenAccept(data -> {
            if (data.isFailed()) {
                LOGGER.error(new RuntimeException(JsonHelper.encode(query) + " : " + data.getError().toString()),
                        "Error running retention query");
            }
        });
        return result;
    }

    private static class RetentionQuery {
        public final boolean approximate;
        private final RetentionAction firstAction;
        private final RetentionAction returningAction;
        private final DateUnit dateUnit;
        private final String dimension;
        private final Integer period;
        private final LocalDate startDate;
        private final LocalDate endDate;
        private final ZoneId timezone;

        @JsonCreator
        public RetentionQuery(@ApiParam("first_action") RetentionAction firstAction,
                              @ApiParam("returning_action") RetentionAction returningAction,
                              @ApiParam("dimension") String dimension,
                              @ApiParam("date_unit") DateUnit dateUnit,
                              @ApiParam(value = "period", required = false) Integer period,
                              @ApiParam("startDate") LocalDate startDate,
                              @ApiParam(value = "timezone", required = false) String timezone,
                              @ApiParam(value = "approximate", required = false) Boolean approximate,
                              @ApiParam("endDate") LocalDate endDate) {
            this.firstAction = firstAction;
            this.returningAction = returningAction;
            this.dateUnit = dateUnit;
            this.dimension = dimension;
            this.period = period;
            this.startDate = startDate;
            this.endDate = endDate;
            this.approximate = Boolean.TRUE.equals(approximate);
            try {
                this.timezone = Optional.ofNullable(timezone)
                        .map(t -> ZoneId.of(t))
                        .orElse(ZoneOffset.UTC);
            } catch (Exception e) {
                throw new RakamException("Timezone is invalid", HttpResponseStatus.BAD_REQUEST);
            }
        }
    }
}
