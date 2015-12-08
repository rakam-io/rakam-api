package org.rakam.bootstrap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Binding;
import com.google.inject.ConfigurationException;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.spi.DefaultElementVisitor;
import com.google.inject.spi.Element;
import com.google.inject.spi.Elements;
import com.google.inject.spi.Message;
import com.google.inject.spi.ProviderInstanceBinding;
import io.airlift.configuration.ConfigurationAwareProvider;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationInspector;
import io.airlift.configuration.WarningsMonitor;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class SystemRegistry {

    private final Set<Module> modules;
    private final Set<Module> installedModules;

    private List<ModuleDescriptor> moduleDescriptors;

    public SystemRegistry(Set<Module> modules, Set<Module> installedModules) {
        this.modules = modules;
        this.installedModules = installedModules;
    }

    public static List<Message> validate(ConfigurationFactory factory, Iterable<? extends Module> modules, WarningsMonitor monitor)
    {
        final List<Message> messages = Lists.newArrayList();

        for (final Element element : Elements.getElements(modules)) {
            element.acceptVisitor(new DefaultElementVisitor<Void>()
            {
                public <T> Void visit(Binding<T> binding)
                {
                    // look for ConfigurationProviders...
                    if (binding instanceof ProviderInstanceBinding) {
                        ProviderInstanceBinding<?> providerInstanceBinding = (ProviderInstanceBinding<?>) binding;
                        Provider<?> provider = providerInstanceBinding.getProviderInstance();
                        if (provider instanceof ConfigurationAwareProvider) {
                            ConfigurationAwareProvider<?> configurationProvider = (ConfigurationAwareProvider<?>) provider;
                            // give the provider the configuration factory
                            configurationProvider.setConfigurationFactory(factory);
                            configurationProvider.setWarningsMonitor(monitor);
                            try {
                                // call the getter which will cause object creation
                                configurationProvider.get();
                            } catch (ConfigurationException e) {
                                // if we got errors, add them to the errors list
                                messages.addAll(e.getErrorMessages().stream()
                                        .map(message -> new Message(singletonList(binding.getSource()), message.getMessage(), message.getCause()))
                                        .collect(Collectors.toList()));
                            }
                        }

                    }

                    return null;
                }
            });
        }
        return messages;
    }

    private List<ModuleDescriptor> createModuleDescriptor() {
        return modules.stream()
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

                    validate(otherConfigurationFactory, ImmutableList.of(module), warning -> {});

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

    public List<ModuleDescriptor> getModules() {
        if(moduleDescriptors == null) {
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
                          @JsonProperty("defaultValue") String defaultValue,
                          @JsonProperty("description") String description) {
            this.property = property;
            this.defaultValue = defaultValue;
            this.description = description;
        }
    }
}
