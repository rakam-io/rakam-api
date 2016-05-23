package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.ApiKeyService.ProjectApiKeys;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.MaterializedView;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.ApiResponse;
import org.rakam.server.http.annotations.ApiResponses;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.CryptUtil;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.util.JsonResponse;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static java.util.Locale.ENGLISH;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.READ_KEY;
import static org.rakam.util.ValidationUtil.checkProject;

@Path("/project")
@Api(value = "/project", nickname = "project", description = "Project operations", tags = "admin")
public class ProjectHttpService extends HttpService {

    private final Metastore metastore;
    private final ContinuousQueryService continuousQueryService;
    private final MaterializedViewService materializedViewService;
    private final ApiKeyService apiKeyService;
    private final ProjectConfig projectConfig;

    @Inject
    public ProjectHttpService(Metastore metastore,
                              ProjectConfig projectConfig,
                              MaterializedViewService materializedViewService,
                              ApiKeyService apiKeyService,
                              ContinuousQueryService continuousQueryService) {
        this.continuousQueryService = continuousQueryService;
        this.materializedViewService = materializedViewService;
        this.apiKeyService = apiKeyService;
        this.metastore = metastore;
        this.projectConfig = projectConfig;
    }

    @ApiOperation(value = "Create project")
    @JsonRequest
    @Path("/create")
    public ProjectApiKeys createProject(@ApiParam(value = "lock_key", required = false) String lockKey, @ApiParam("name") String name) {
        if(!Objects.equals(projectConfig.getLockKey(), lockKey)) {
            throw new RakamException("Lock key is invalid", FORBIDDEN);
        }

        checkProject(name);
        metastore.createProject(name.toLowerCase(ENGLISH));
        return transformKeys(apiKeyService.createApiKeys(name));
    }

    @ApiOperation(value = "Delete project",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/delete")
    public JsonResponse deleteProject(@Named("project") String project) {
        checkProject(project);
        metastore.deleteProject(project.toLowerCase(ENGLISH));

        List<ContinuousQuery> list = continuousQueryService.list(project);
        for (ContinuousQuery continuousQuery : list) {
            continuousQueryService.delete(project, continuousQuery.tableName);
        }

        List<MaterializedView> views = materializedViewService.list(project);
        for (MaterializedView view : views) {
            materializedViewService.delete(project, view.tableName);
        }

        apiKeyService.revokeAllKeys(project);

        return JsonResponse.success();
    }

    @ApiOperation(value = "Get project stats")
    @JsonRequest
    @IgnorePermissionCheck
    @Path("/stats")
    public Map<String, Metastore.Stats> getStats(@BodyParam List<String> apiKeys) {

        Map<String, String> keys = new LinkedHashMap<>();
        for (String apiKey : apiKeys) {
            String project;
            try {
                project = apiKeyService.getProjectOfApiKey(apiKey, READ_KEY);
            } catch (RakamException e) {
                if(e.getStatusCode() == FORBIDDEN) {
                    continue;
                }
                throw e;
            }
            keys.put(project, apiKey);
        }

        Map<String, Metastore.Stats> stats = metastore.getStats(keys.keySet());
        if(stats == null) {
            return ImmutableMap.of();
        }
        return stats.entrySet().stream()
                .collect(Collectors.toMap(e -> keys.get(e.getKey()), e -> e.getValue()));
    }

    public static class Project {
        public final String project;
        public final String apiKey;

        public Project(String project, String apiKey) {
            this.project = project;
            this.apiKey = apiKey;
        }
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
    public List<SchemaField> addFieldsToSchema(@Named("project") String project,
                                               @ApiParam("collection") String collection,
                                               @ApiParam("fields") Set<SchemaField> fields) {
        return metastore.getOrCreateCollectionFieldList(project, collection, fields);
    }

    @JsonRequest
    @ApiOperation(value = "Add fields to collections by transforming other schemas",
            authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/schema/add/custom")
    public List<SchemaField> addCustomFieldsToSchema(@Named("project") String project,
                                                     @ApiParam("collection") String collection,
                                                     @ApiParam("schema_type") SchemaConverter type,
                                                     @ApiParam("schema") String schema) {
        return metastore.getOrCreateCollectionFieldList(project, collection, type.getMapper().apply(schema));
    }

    @JsonRequest
    @ApiOperation(value = "Get collection schema",
            authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/schema")
    public List<Collection> schema(@Named("project") String project,
                                   @ApiParam(value = "names", required = false) Set<String> names) {
        return metastore.getCollections(project).entrySet().stream()
                .filter(entry -> names == null || names.contains(entry.getKey()))
                .map(entry -> new Collection(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    @JsonRequest
    @ApiOperation(value = "Create API Keys",
            authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/create-api-keys")
    public ProjectApiKeys createApiKeys(@Named("project") String project) {
        return transformKeys(apiKeyService.createApiKeys(project));
    }

    private ProjectApiKeys transformKeys(ProjectApiKeys apiKeys) {
        if (projectConfig.getPassphrase() == null) {
            return ProjectApiKeys.create(apiKeys.masterKey(), apiKeys.readKey(), apiKeys.writeKey());
        } else {
            return ProjectApiKeys.create(
                    CryptUtil.encryptAES(apiKeys.masterKey(), projectConfig.getPassphrase()),
                    CryptUtil.encryptAES(apiKeys.masterKey(), projectConfig.getPassphrase()),
                    CryptUtil.encryptAES(apiKeys.masterKey(), projectConfig.getPassphrase()));
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get collection names",
            authorizations = @Authorization(value = "read_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/collection")
    public Set<String> collections(@Named("project") String project) {
        return metastore.getCollectionNames(project);
    }

    @JsonRequest
    @ApiOperation(value = "Revoke API Keys",
            authorizations = @Authorization(value = "master_key"))
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Project does not exist.")})
    @Path("/revoke-api-keys")
    public JsonResponse revokeApiKeys(@Named("project") String project, @HeaderParam("api_key") String masterKey) {
        apiKeyService.revokeApiKeys(project, masterKey);
        return JsonResponse.success();
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
