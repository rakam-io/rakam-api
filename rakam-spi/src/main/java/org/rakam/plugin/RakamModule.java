package org.rakam.plugin;

import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.util.ConditionalModule;

import javax.validation.constraints.NotNull;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigBinder.configBinder;

public abstract class RakamModule
        implements ConfigurationAwareModule {
    private ConfigurationFactory configurationFactory;
    private Binder binder;

    @Override
    public synchronized void setConfigurationFactory(ConfigurationFactory configurationFactory) {
        this.configurationFactory = checkNotNull(configurationFactory, "configurationFactory is null");
    }

    @Override
    public final synchronized void configure(Binder binder) {
        checkState(this.binder == null, "re-entry not allowed");
        this.binder = checkNotNull(binder, "binder is null");

        try {
            ConditionalModule annotation = this.getClass().getAnnotation(ConditionalModule.class);
            if (annotation != null) {
                configurationFactory.consumeProperty(annotation.config());
                String value = Optional.ofNullable(configurationFactory.getProperties().get(annotation.config()))
                        .map(v -> v.trim()).orElse(null);
                if (!Objects.equals(annotation.value(), value)) {
                    return;
                }
            }

            setup(binder);
        } finally {
            this.binder = null;
        }
    }

    protected synchronized <T> T buildConfigObject(Class<T> configClass) {
        configBinder(binder).bindConfig(configClass);
        return configurationFactory.build(configClass);
    }

    protected synchronized String getConfig(String config) {
        String value = configurationFactory.getProperties().get(config);
        configurationFactory.consumeProperty(config);
        return value;
    }

    protected synchronized <T> T buildConfigObject(Class<T> configClass, String prefix) {
        configBinder(binder).bindConfig(configClass,
                prefix != null ? Names.named(prefix) : null, prefix);
        try {
            Method method = configurationFactory.getClass().getDeclaredMethod("build", Class.class, Optional.class, ConfigDefaults.class);
            method.setAccessible(true);
            Object invoke = method.invoke(configurationFactory, configClass, Optional.of(prefix), ConfigDefaults.noDefaults());
            Field instance = invoke.getClass().getDeclaredField("instance");
            instance.setAccessible(true);
            return (T) instance.get(invoke);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            throw new IllegalStateException("Internal error related to airlift.configuration library", e);
        }
    }

    protected synchronized void install(RakamModule module) {
        module.setConfigurationFactory(configurationFactory);
        binder.install(module);
    }

    protected abstract void setup(Binder binder);

    @NotNull
    public abstract String name();

    public abstract String description();
}