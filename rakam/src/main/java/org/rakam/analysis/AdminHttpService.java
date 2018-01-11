package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.Mapper;
import org.rakam.bootstrap.SystemRegistry;
import org.rakam.bootstrap.SystemRegistry.ModuleDescriptor;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.ActiveModuleListBuilder;
import org.rakam.ui.ActiveModuleListBuilder.ActiveModuleList;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.*;
import java.util.stream.Collectors;

@Path("/admin")
@Api(value = "/admin", nickname = "admin", description = "System operations", tags = "admin")
public class AdminHttpService
        extends HttpService {
    private final SystemRegistry systemRegistry;
    private final ActiveModuleList activeModules;
    private final ProjectConfig projectConfig;
    private final Set<EventMapper> eventMappers;

    @Inject
    public AdminHttpService(SystemRegistry systemRegistry, Set<EventMapper> eventMappers, ProjectConfig projectConfig, ActiveModuleListBuilder activeModuleListBuilder) {
        this.systemRegistry = systemRegistry;
        this.projectConfig = projectConfig;
        this.eventMappers = eventMappers;
        activeModules = activeModuleListBuilder.build();
    }

    @ApiOperation(value = "List installed modules",
            authorizations = @Authorization(value = "master_key")
    )
    @GET
    @Path("/configurations")
    @JsonRequest
    public List<ModuleDescriptor> getConfigurations() {
        return systemRegistry.getModules();
    }

    @ApiOperation(value = "List event mappers",
            authorizations = @Authorization(value = "master_key")
    )
    @GET
    @Path("/event_mappers")
    @JsonRequest
    public List<EventMapperDescription> getEventMappers() {
        return eventMappers.stream().map(mapper -> {
            Mapper annotation = mapper.getClass().getAnnotation(Mapper.class);
            String name;
            String description;
            if (annotation != null) {
                name = annotation.name();
                description = annotation.description();
            } else {
                name = mapper.getClass().getSimpleName();
                description = "";
            }

            FieldDependencyBuilder builder = new FieldDependencyBuilder();
            mapper.addFieldDependency(builder);
            FieldDependencyBuilder.FieldDependency build = builder.build();
            return new EventMapperDescription(name, description,
                    build.dependentFields.isEmpty() ? null : build.dependentFields,
                    build.constantFields.isEmpty() ? null : build.constantFields);
        }).collect(Collectors.toList());
    }

    @ApiOperation(value = "Get types",
            authorizations = @Authorization(value = "master_key")
    )

    @GET
    @JsonRequest
    @Path("/types")
    public Map<String, String> getTypes() {
        return Arrays.stream(FieldType.values()).collect(Collectors.toMap(FieldType::name, FieldType::getPrettyName));
    }

    @ApiOperation(value = "Check lock key",
            authorizations = @Authorization(value = "master_key")
    )
    @JsonRequest
    @Path("/lock_key")
    public boolean checkLockKey(@ApiParam(value = "lock_key", required = false) String lockKey) {
        return Objects.equals(lockKey, projectConfig.getLockKey());
    }

    @Path("/modules")
    @GET
    @IgnoreApi
    @ApiOperation(value = "List installed modules for Rakam UI",
            authorizations = @Authorization(value = "master_key")
    )
    public ActiveModuleList modules() {
        return activeModules;
    }

    public static class EventMapperDescription {
        public final String name;
        public final String description;
        public final Map<String, List<SchemaField>> dependentFields;
        public final Set<SchemaField> constantFields;

        @JsonCreator
        public EventMapperDescription(String name, String description, Map<String, List<SchemaField>> dependentFields, Set<SchemaField> constantFields) {
            this.name = name;
            this.description = description;
            this.dependentFields = dependentFields;
            this.constantFields = constantFields;
        }
    }
}
