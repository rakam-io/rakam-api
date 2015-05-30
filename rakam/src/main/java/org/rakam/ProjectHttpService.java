package org.rakam;

import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.SystemEventListener;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 18/05/15 22:02.
 */
@Path("/project")
@Api(value = "/project", description = "System operations", tags = "admin")
public class ProjectHttpService extends HttpService {

    private final Set<SystemEventListener> systemEventListeners;
    private final Metastore metastore;

    @Inject
    public ProjectHttpService(Metastore metastore, Set<SystemEventListener> systemEventListeners) {
        this.metastore = metastore;
        this.systemEventListeners = systemEventListeners;
    }

    @ApiOperation(value = "Create project",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @JsonRequest
    @Path("/create")
    public JsonResponse createProject(@ApiParam(name="name") String name) {
        metastore.createProject(name);
        systemEventListeners.forEach(listener -> listener.onCreateProject(name));
        return JsonResponse.success();
    }

    @ApiOperation(value = "List created projects",
            authorizations = @Authorization(value = "api_key", type = "api_key")
    )
    @GET
    @Path("/list")
    public List<String> getProjects() {
        return metastore.getProjects();
    }

    /**
     * {"collections":[{"name":"pageView","fields":[{"name":"url","type":"STRING","nullable":true},{"name":"id","type":"LONG","nullable":false}]}]}
     * @apiExample {curl} Example usage:
     * curl 'http://localhost:9999/event/schema' -H 'Content-Type: text/event-stream;charset=UTF-8' --data-binary '{"project": "projectId"}'
     */
    @JsonRequest
    @ApiOperation(value = "Get collection schema")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/schema")
    public List<Collection> schema(@ApiParam(name = "project", required = true) String project) {
        return metastore.getCollections(project).entrySet().stream()
                // ignore system tables
                .filter(entry -> !entry.getKey().startsWith("_"))
                .map(entry -> new Collection(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public static class Collection {
        public final String name;
        public final List<SchemaField> fields;

        public Collection(String name, List<SchemaField> fields) {
            this.name = name;
            this.fields = fields;
        }
    }
}
