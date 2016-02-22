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
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.RetentionQueryExecutor.DateUnit;
import org.rakam.analysis.RetentionQueryExecutor.RetentionAction;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.plugin.ProjectItem;
import org.rakam.analysis.QueryHttpService;
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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Path("/retention")
@Api(value = "/retention", nickname = "retentionAnalyzer", tags = "retention")
public class RetentionAnalyzerHttpService extends HttpService {
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
    @IgnorePermissionCheck
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

    @ApiOperation(value = "Execute query",
            authorizations = @Authorization(value = "read_key")
    )
    @POST
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> execute(@ParamBody RetentionQuery query) {
          return retentionQueryExecutor.query(query.project,
                        query.connectorField,
                        Optional.ofNullable(query.firstAction),
                        Optional.ofNullable(query.returningAction),
                        query.dateUnit,
                        Optional.ofNullable(query.dimension),
                        query.startDate,
                        query.endDate).getResult();
    }

    private static class RetentionQuery implements ProjectItem {
        private final String project;
        private final String connectorField;
        private final RetentionAction firstAction;
        private final RetentionAction returningAction;
        private final DateUnit dateUnit;
        private final String dimension;
        private final LocalDate startDate;
        private final LocalDate endDate;

        @JsonCreator
        public RetentionQuery(@ApiParam(name = "project") String project,

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

        @Override
        public String project() {
            return project;
        }
    }
}
