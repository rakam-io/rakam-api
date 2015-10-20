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

import org.rakam.collection.event.metastore.Metastore;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@Path("/event-explorer")
@Api(value = "/event-explorer", description = "Event explorer module", tags = "event-explorer")
public class EventExplorerHttpService extends HttpService {
    private final EventExplorer eventExplorer;
    private final Metastore metastore;

    @Inject
    public EventExplorerHttpService(EventExplorer eventExplorer, Metastore metastore) {
        this.eventExplorer = eventExplorer;
        this.metastore = metastore;
    }

    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/statistics")
    public CompletableFuture<QueryResult> getEventStatistics(@ApiParam(name = "project", required = true) String project,
                                                             @ApiParam(name = "dimension") String dimension,
                                                             @ApiParam(name = "startDate", required = true) LocalDate startDate,
                                                             @ApiParam(name = "endDate", required = true) LocalDate endDate,
                                                             @HeaderParam(value = "read_key") String readKey) {
        if(!metastore.checkPermission(project, Metastore.AccessKeyType.READ_KEY, readKey)) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
        return eventExplorer.getEventStatistics(project, Optional.ofNullable(dimension), startDate, endDate);
    }


    @JsonRequest
    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/extra_dimensions")
    public List<String> getExtraDimensions(@ApiParam(name = "project", required = true) String project, @HeaderParam(value = "read_key") String readKey) {
        if(!metastore.checkPermission(project, Metastore.AccessKeyType.READ_KEY, readKey)) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
        return eventExplorer.getExtraDimensions(project);
    }

    @JsonRequest
    @ApiOperation(value = "Event statistics",
            authorizations = @Authorization(value = "read_key")
    )
    @Path("/event_dimensions")
    public List<String> getEventDimensions(@ApiParam(name = "project", required = true) String project, @HeaderParam(value = "read_key") String readKey) {
        if(!metastore.checkPermission(project, Metastore.AccessKeyType.READ_KEY, readKey)) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
        return eventExplorer.getEventDimensions(project);
    }

    @ApiOperation(value = "Perform simple query on event data",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/analyze")
    public CompletableFuture<QueryResult> execute(@ApiParam(name = "project") String project,
                                                  @ApiParam(name = "measure", required = false) EventExplorer.Measure measure,
                                                  @ApiParam(name = "grouping", required = false) EventExplorer.Reference grouping,
                                                  @ApiParam(name = "segment", required = false) EventExplorer.Reference segment,
                                                  @ApiParam(name = "filterExpression", required = false) String filterExpression,
                                                  @ApiParam(name = "startDate") LocalDate startDate,
                                                  @ApiParam(name = "endDate") LocalDate endDate,
                                                  @ApiParam(name="collections") List<String> collections,
                                                  @HeaderParam(value = "read_key") String readKey) {
        checkArgument(collections.size() > 0, "collections array is empty");
        checkArgument(!measure.column.equals("time"), "measure column value cannot be 'time'");
        if(!metastore.checkPermission(project, Metastore.AccessKeyType.READ_KEY, readKey)) {
            throw new RakamException(UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }
        return eventExplorer.analyze(project,
                                            collections,
                                            measure, grouping,
                                            segment, filterExpression,
                                            startDate, endDate);
    }
}
