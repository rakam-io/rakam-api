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
import org.rakam.analysis.RetentionQueryExecutor.DateUnit;
import org.rakam.analysis.RetentionQueryExecutor.RetentionAction;
import org.rakam.report.QueryHttpService;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.Optional;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/08/15 06:40.
 */
@Path("/retention")
@Api(value = "/retention", description = "Retention Analyzer module", tags = "event")
public class RetentionAnalyzerHttpService extends HttpService {
    private final RetentionQueryExecutor retentionQueryExecutor;
    private final QueryHttpService queryService;

    @Inject
    public RetentionAnalyzerHttpService(RetentionQueryExecutor retentionQueryExecutor, QueryHttpService queryService) {
        this.retentionQueryExecutor = retentionQueryExecutor;
        this.queryService = queryService;
    }

    @ApiOperation(value = "Analyze event data-set",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @GET
    @Path("/analyze")
    public void execute(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, RetentionQuery.class, (query) ->
                retentionQueryExecutor.query(query.project,
                        Optional.ofNullable(query.firstAction),
                        Optional.ofNullable(query.returningAction),
                        query.dateUnit,
                        Optional.ofNullable(query.dimension),
                        query.startDate,
                        query.endDate));
    }

    private static class RetentionQuery {
        public final @ApiParam(name = "project", required = true) String project;
        public final @ApiParam(name = "first_action", required = true) RetentionAction firstAction;
        public final @ApiParam(name = "returning_action", required = true) RetentionAction returningAction;
        public final @ApiParam(name = "dimension", required = false) String dimension;
        public final @ApiParam(name = "date_unit", required = false) DateUnit dateUnit;
        public final @ApiParam(name = "startDate", required = true) LocalDate startDate;
        public final @ApiParam(name = "endDate", required = true) LocalDate endDate;

        @JsonCreator
        private RetentionQuery(@JsonProperty("project") String project,
                            @JsonProperty("first_action") RetentionAction firstAction,
                            @JsonProperty("returning_action") RetentionAction returningAction,
                            @JsonProperty("date_unit") DateUnit dateUnit,
                            @JsonProperty("dimension") String dimension,
                            @JsonProperty("startDate") LocalDate startDate,
                            @JsonProperty("endDate") LocalDate endDate) {
            this.project = project;
            this.firstAction = firstAction;
            this.returningAction = returningAction;
            this.dateUnit = dateUnit;
            this.dimension = dimension;
            this.startDate = startDate;
            this.endDate = endDate;
        }
    }
}
