package org.rakam;

import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonResponse;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.rakam.util.ValidationUtil.checkProject;

@Path("/project")
@Api(value = "/project", description = "Project operations", tags = "admin")
public class ProjectHttpService extends HttpService {

    private final Metastore metastore;

    @Inject
    public ProjectHttpService(Metastore metastore) {
        this.metastore = metastore;
    }

    @ApiOperation(value = "Create project",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/create")
    public JsonResponse createProject(@ApiParam(name="name") String name) {
        checkProject(name);
        metastore.createProject(name.toLowerCase(Locale.ENGLISH));
        return JsonResponse.success();
    }

    @ApiOperation(value = "List created projects",
            authorizations = @Authorization(value = "read_key")
    )
    @GET
    @Path("/list")
    public Set<String> getProjects() {
        return metastore.getProjects();
    }

    @JsonRequest
    @ApiOperation(value = "Get collection schema",
            authorizations = @Authorization(value = "read_key"))
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
