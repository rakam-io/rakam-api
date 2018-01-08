package org.rakam.bootstrap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationInspector;
import org.rakam.plugin.RakamModule;
import org.rakam.util.ConditionalModule;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SystemRegistry {
    private final Set<Module> installedModules;
    private final Set<Module> allModules;

    private List<ModuleDescriptor> moduleDescriptors;

    public SystemRegistry(Set<Module> allModules, Set<Module> installedModules) {
        this.allModules = allModules;
        this.installedModules = installedModules;
    }

    private List<ModuleDescriptor> createModuleDescriptor() {
        return (allModules == null ? installedModules : allModules).stream()
                .filter(module -> module instanceof RakamModule)
                .map(module -> {
                    RakamModule rakamModule = (RakamModule) module;
                    ConditionalModule annotation = rakamModule.getClass().getAnnotation(ConditionalModule.class);
                    Optional<SystemRegistry.ModuleDescriptor.Condition> condition;
                    if (annotation != null) {
                        condition = Optional.of(new SystemRegistry.ModuleDescriptor.Condition(annotation.config(), annotation.value()));
                    } else {
                        condition = Optional.empty();
                    }

                    ConfigurationFactory otherConfigurationFactory = new ConfigurationFactory(ImmutableMap.of());

                    // process modules and add used properties to ConfigurationFactory

                    otherConfigurationFactory.validateRegisteredConfigurationProvider();

                    ImmutableList.Builder<ConfigItem> attributesBuilder = ImmutableList.builder();

                    ConfigurationInspector configurationInspector = new ConfigurationInspector();
                    for (ConfigurationInspector.ConfigRecord<?> record : configurationInspector.inspect(otherConfigurationFactory)) {
                        for (ConfigurationInspector.ConfigAttribute attribute : record.getAttributes()) {
                            attributesBuilder.add(new ConfigItem(attribute.getPropertyName(), attribute.getDefaultValue(), attribute.getDescription()));
                        }
                    }

                    return new SystemRegistry.ModuleDescriptor(
                            rakamModule.name(),
                            rakamModule.description(),
                            rakamModule.getClass().getName(),
                            installedModules.contains(module),
                            condition,
                            attributesBuilder.build());
                }).collect(Collectors.toList());
    }

    public synchronized List<ModuleDescriptor> getModules() {
        if (moduleDescriptors == null) {
            this.moduleDescriptors = createModuleDescriptor();
        }

        return moduleDescriptors;
    }

    public static class ModuleDescriptor {
        public final String name;
        public final String description;
        public final String className;
        public final boolean isActive;
        public final Optional<Condition> condition;
        public final List<ConfigItem> properties;

        @JsonCreator
        public ModuleDescriptor(@JsonProperty("name") String name,
                                @JsonProperty("description") String description,
                                @JsonProperty("className") String className,
                                @JsonProperty("isActive") boolean isActive,
                                @JsonProperty("condition") Optional<Condition> condition,
                                @JsonProperty("properties") List<ConfigItem> properties) {
            this.name = name;
            this.description = description;
            this.className = className;
            this.isActive = isActive;
            this.condition = condition;
            this.properties = properties;
        }

        public static class Condition {
            public final String property;
            public final String expectedValue;

            public Condition(@JsonProperty("property") String property,
                             @JsonProperty("expectedValue") String expectedValue) {
                this.property = property;
                this.expectedValue = expectedValue;
            }
        }
    }

    public static class ConfigItem {
        public final String property;
        public final String defaultValue;
        public final String description;

        @JsonCreator
        public ConfigItem(@JsonProperty("property") String property,
                          @JsonProperty(value = "defaultValue") String defaultValue,
                          @JsonProperty(value = "description") String description) {
            this.property = property;
            this.defaultValue = defaultValue;
            this.description = description;
        }
    }
}
