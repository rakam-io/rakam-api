package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.ApiKeyService.ProjectApiKeys;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.util.CryptUtil;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.*;
import static org.rakam.util.ValidationUtil.checkProject;

@Path("/project")
@Api(value = "/project", nickname = "project", description = "Project operations", tags = "admin")
public class ProjectHttpService
        extends HttpService {

    private final Metastore metastore;
    private final MaterializedViewService materializedViewService;
    private final ApiKeyService apiKeyService;
    private final ProjectConfig projectConfig;
    private final SchemaChecker schemaChecker;

    @Inject
    public ProjectHttpService(Metastore metastore,
                              ProjectConfig projectConfig,
                              SchemaChecker schemaChecker,
                              MaterializedViewService materializedViewService,
                              ApiKeyService apiKeyService) {
        this.materializedViewService = materializedViewService;
        this.apiKeyService = apiKeyService;
        this.metastore = metastore;
        this.schemaChecker = schemaChecker;
        this.projectConfig = projectConfig;
    }

    @ApiOperation(value = "Create project")
    @JsonRequest
    @Path("/create")
    public ProjectApiKeys createProject(@ApiParam(value = "lock_key", required = false) String lockKey, @ApiParam("name") String name) {
        if (!Objects.equals(projectConfig.getLockKey(), lockKey)) {
            lockKey = lockKey == null ? "" : (lockKey.isEmpty() ? null : lockKey);
            if (!Objects.equals(projectConfig.getLockKey(), lockKey)) {
                throw new RakamException("Lock key is invalid", FORBIDDEN);
            }
        }
        String project;
        try {
            project = checkProject(name);
        } catch (IllegalArgumentException e) {
            throw new RakamException(e.getMessage(), BAD_REQUEST);
        }
        if (metastore.getProjects().contains(project)) {
            throw new RakamException("The project already exists.", BAD_REQUEST);
        }

        metastore.createProject(project);
        return transformKeys(apiKeyService.createApiKeys(project));
    }

    @ApiOperation(value = "Delete project",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @DELETE
    @Path("/delete")
    public SuccessMessage deleteProject(@Named("project") RequestContext context) {
        if (!projectConfig.getAllowProjectDeletion()) {
            throw new RakamException("Project deletion is disabled, you can enable it with `allow-project-deletion` config.", NOT_IMPLEMENTED);
        }
        checkProject(context.project);
        metastore.deleteProject(context.project);

        List<MaterializedView> views = materializedViewService.list(context.project);
        for (MaterializedView view : views) {
            materializedViewService.delete(context, view.tableName);
        }

        apiKeyService.revokeAllKeys(context.project);

        return SuccessMessage.success();
    }

    @ApiOperation(value = "Get project stats")
    @JsonRequest
    @Path("/stats")
    public Map<String, Metastore.Stats> getStats(@BodyParam List<String> apiKeys) {
        Map<String, String> keys = new LinkedHashMap<>();
        for (String apiKey : apiKeys) {
            String project;
            try {
                project = apiKeyService.getProjectOfApiKey(apiKey, READ_KEY);
            } catch (RakamException e) {
                if (e.getStatusCode() == FORBIDDEN) {
                    continue;
                }
                throw e;
            }
            keys.put(project, apiKey);
        }

        Map<String, Metastore.Stats> stats = metastore.getStats(keys.keySet());
        if (stats == null) {
            return ImmutableMap.of();
        }
        return stats.entrySet().stream()
                .collect(Collectors.toMap(e -> keys.get(e.getKey()), e -> e.getValue()));
    }

    @ApiOperation(value = "List created projects",
            authorizations = @Authorization(value = "read_key")
    )
    @JsonRequest
    @Path("/list")
    public Set<String> getProjects(@ApiParam(value = "lock_key", required = false) String lockKey) {
        if (!Objects.equals(projectConfig.getLockKey(), lockKey)) {
            throw new RakamException("Lock key is invalid", FORBIDDEN);
        }

        return metastore.getProjects();
    }

    @JsonRequest
    @ApiOperation(value = "Add fields to collections",
            authorizations = @Authorization(value = "master_key"))

    @Path("/schema/add")
    public List<SchemaField> addFieldsToSchema(@Named("project") RequestContext context,
                                               @ApiParam("collection") String collection,
                                               @ApiParam("fields") Set<SchemaField> fields) {
        return metastore.getOrCreateCollectionFields(context.project, collection,
                schemaChecker.checkNewFields(collection, fields));
    }

    @JsonRequest
    @ApiOperation(value = "Add fields to collections by transforming other schemas",
            authorizations = @Authorization(value = "master_key"))

    @Path("/schema/add/custom")
    public List<SchemaField> addCustomFieldsToSchema(@Named("project") RequestContext context,
                                                     @ApiParam("collection") String collection,
                                                     @ApiParam("schema_type") SchemaConverter type,
                                                     @ApiParam("schema") String schema) {
        return metastore.getOrCreateCollectionFields(context.project, collection,
                schemaChecker.checkNewFields(collection, type.getMapper().apply(schema)));
    }

    @JsonRequest
    @ApiOperation(value = "Get collection schema",
            authorizations = @Authorization(value = "read_key"))

    @Path("/schema")
    public List<Collection> schema(@Named("project") RequestContext context,
                                   @ApiParam(value = "names", required = false) Set<String> names) {
        return metastore.getCollections(context.project).entrySet().stream()
                .filter(entry -> names == null || names.contains(entry.getKey()))
                .map(entry -> new Collection(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    @JsonRequest
    @ApiOperation(value = "Create API Keys",
            authorizations = @Authorization(value = "master_key"))

    @Path("/create-api-keys")
    public ProjectApiKeys createApiKeys(@Named("project") RequestContext context) {
        return transformKeys(apiKeyService.createApiKeys(context.project));
    }

    @JsonRequest
    @ApiOperation(value = "Check API Keys")
    @Path("/check-api-keys")
    public List<Boolean> checkApiKeys(@ApiParam("keys") List<ProjectApiKeys> keys, @ApiParam("project") String project) {
        return keys.stream().map(key -> {
            try {
                Consumer<String> stringConsumer = e -> {
                    if (!e.equals(project.toLowerCase(Locale.ENGLISH))) {
                        throw new RakamException(FORBIDDEN);
                    }
                };
                Optional.ofNullable(key.masterKey()).map(k -> apiKeyService.getProjectOfApiKey(k, MASTER_KEY)).ifPresent(stringConsumer);
                Optional.ofNullable(key.readKey()).map(k -> apiKeyService.getProjectOfApiKey(k, READ_KEY)).ifPresent(stringConsumer);
                Optional.ofNullable(key.writeKey()).map(k -> apiKeyService.getProjectOfApiKey(k, WRITE_KEY)).ifPresent(stringConsumer);
                return true;
            } catch (RakamException e) {
                return false;
            }
        }).collect(Collectors.toList());
    }

    private ProjectApiKeys transformKeys(ProjectApiKeys apiKeys) {
        if (projectConfig.getPassphrase() == null) {
            return ProjectApiKeys.create(apiKeys.masterKey(), apiKeys.readKey(), apiKeys.writeKey());
        } else {
            return ProjectApiKeys.create(
                    CryptUtil.encryptAES(apiKeys.masterKey(), projectConfig.getPassphrase()),
                    CryptUtil.encryptAES(apiKeys.readKey(), projectConfig.getPassphrase()),
                    CryptUtil.encryptAES(apiKeys.writeKey(), projectConfig.getPassphrase()));
        }
    }

    @JsonRequest
    @ApiOperation(value = "Get possible attribute values",
            authorizations = @Authorization(value = "read_key"))
    @Path("/attributes")
    public CompletableFuture<List<String>> attributes(@Named("project") RequestContext context,
                                                      @ApiParam("collection") String collection,
                                                      @ApiParam("attribute") String attribute,
                                                      @ApiParam(value = "startDate", required = false) LocalDate startDate,
                                                      @ApiParam(value = "endDate", required = false) LocalDate endDate,
                                                      @ApiParam(value = "filter", required = false) String filter) {
        return metastore.getAttributes(context.project, collection, attribute, Optional.ofNullable(startDate),
                Optional.ofNullable(endDate), Optional.ofNullable(filter));
    }

    @JsonRequest
    @ApiOperation(value = "Get collection names",
            authorizations = @Authorization(value = "read_key"))

    @Path("/collection")
    public Set<String> collections(@Named("project") RequestContext context) {
        return metastore.getCollectionNames(context.project);
    }

    @JsonRequest
    @ApiOperation(value = "Revoke API Keys")
    @Path("/revoke-api-keys")
    public SuccessMessage revokeApiKeys(@ApiParam("project") String project, @ApiParam("master_key") String masterKey) {
        apiKeyService.revokeApiKeys(project, masterKey);
        return SuccessMessage.success();
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
