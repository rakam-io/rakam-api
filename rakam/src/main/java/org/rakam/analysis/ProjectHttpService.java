package org.rakam.analysis;

import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
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
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;
import static org.rakam.util.ValidationUtil.checkProject;

@Path("/project")
@Api(value = "/project", nickname = "project", description = "Project operations", tags = "admin")
public class ProjectHttpService extends HttpService {

    private final Metastore metastore;
    private final ContinuousQueryService continuousQueryService;
    private final MaterializedViewService materializedViewService;

    @Inject
    public ProjectHttpService(Metastore metastore, MaterializedViewService materializedViewService, ContinuousQueryService continuousQueryService) {
        this.continuousQueryService = continuousQueryService;
        this.materializedViewService = materializedViewService;
        this.metastore = metastore;
    }

    @ApiOperation(value = "Create project",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/create")
    public JsonResponse createProject(@ApiParam(name="name") String name) {
        checkProject(name);
        metastore.createProject(name.toLowerCase(ENGLISH));
        return JsonResponse.success();
    }

    @ApiOperation(value = "Delete project",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/delete")
    public JsonResponse deleteProject(@ApiParam(name="project") String project) {
        checkProject(project);
        metastore.deleteProject(project.toLowerCase(ENGLISH));

        List<ContinuousQuery> list = continuousQueryService.list(project);
        for (ContinuousQuery continuousQuery : list) {
            if(!continuousQueryService.delete(continuousQuery.project,
                    continuousQuery.tableName).join()) {
            }
        }

        List<MaterializedView> views = materializedViewService.list(project);
        for (MaterializedView view : views) {
            materializedViewService.delete(view.project, view.tableName);
        }

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
    @ApiOperation(value = "Add fields to collections",
            authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/schema/add")
    public List<SchemaField> addFieldsToSchema(@ApiParam(name = "project") String project,
                                               @ApiParam(name = "collection") String collection,
                                               @ApiParam(name = "fields") Set<SchemaField> fields) {
        return metastore.getOrCreateCollectionFieldList(project, collection, fields);
    }

    @JsonRequest
    @ApiOperation(value = "Get collection schema",
            authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/schema")
    public List<Collection> schema(@ApiParam(name = "project") String project,
                                   @ApiParam(name = "names", required = false) Set<String> names) {
        return metastore.getCollections(project).entrySet().stream()
                .filter(entry -> names == null || names.contains(entry.getKey()))
                .map(entry -> new Collection(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    @JsonRequest
    @ApiOperation(value = "Get collection names",
            authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/collection")
    public Set<String> collections(@ApiParam(name = "project") String project) {
        return metastore.getCollectionNames(project);
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
