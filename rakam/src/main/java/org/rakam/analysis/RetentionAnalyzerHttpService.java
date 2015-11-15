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
import org.rakam.analysis.RetentionQueryExecutor.DateUnit;
import org.rakam.analysis.RetentionQueryExecutor.RetentionAction;
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
import java.util.Optional;

@Path("/retention")
@Api(value = "/retention", tags = "retention")
public class RetentionAnalyzerHttpService extends HttpService {
    private final RetentionQueryExecutor retentionQueryExecutor;
    private final QueryHttpService queryService;

    @Inject
    public RetentionAnalyzerHttpService(RetentionQueryExecutor retentionQueryExecutor, QueryHttpService queryService) {
        this.retentionQueryExecutor = retentionQueryExecutor;
        this.queryService = queryService;
    }

    @ApiOperation(value = "Execute query",
            authorizations = @Authorization(value = "read_key")
    )
    @GET
    @Path("/analyze")
    public void execute(RakamHttpRequest request) {
        queryService.handleServerSentQueryExecution(request, RetentionQuery.class, (query) ->
                retentionQueryExecutor.query(query.project,
                        query.connectorField,
                        Optional.ofNullable(query.firstAction),
                        Optional.ofNullable(query.returningAction),
                        query.dateUnit,
                        Optional.ofNullable(query.dimension),
                        query.startDate,
                        query.endDate));
    }

    private static class RetentionQuery {
        private final String project;
        private final String connectorField;
        private final RetentionAction firstAction;
        private final RetentionAction returningAction;
        private final DateUnit dateUnit;
        private final String dimension;
        private final LocalDate startDate;
        private final LocalDate endDate;

        @JsonCreator
        private RetentionQuery(@ApiParam(name = "project") String project,

                               @ApiParam(name = "connector_field") String connectorField,

                               @ApiParam(name = "first_action") RetentionAction firstAction,

                               @ApiParam(name = "returning_action") RetentionAction returningAction,

                               @ApiParam(name = "dimension") String dimension,
                               @ApiParam(name = "date_unit") DateUnit dateUnit,
                               @ApiParam(name = "startDate") LocalDate startDate,
                               @ApiParam(name = "endDate") LocalDate endDate) {
            this.project = project;
            this.connectorField = connectorField;
            this.firstAction = firstAction;
            this.returningAction = returningAction;
            this.dateUnit = dateUnit;
            this.dimension = dimension;
            this.startDate = startDate;
            this.endDate = endDate;
        }
    }
}
