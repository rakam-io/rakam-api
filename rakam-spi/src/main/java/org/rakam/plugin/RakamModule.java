package org.rakam.plugin;

import com.google.inject.Binder;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;


public abstract class RakamModule implements ConfigurationAwareModule {
    private ConfigurationFactory configurationFactory;
    private Binder binder;

    @Override
    public synchronized void setConfigurationFactory(ConfigurationFactory configurationFactory)
    {
        this.configurationFactory = checkNotNull(configurationFactory, "configurationFactory is null");
    }

    @Override
    public final synchronized void configure(Binder binder)
    {
        checkState(this.binder == null, "re-entry not allowed");
        this.binder = checkNotNull(binder, "binder is null");
        try {
            setup(binder);
        }
        finally {
            this.binder = null;
        }
    }

    protected synchronized <T> T buildConfigObject(Class<T> configClass)
    {
        ConfigurationModule.bindConfig(binder).to(configClass);
        return configurationFactory.build(configClass);
    }

    protected synchronized String getConfig(String config)
    {
        String value = configurationFactory.getProperties().get(config);
        configurationFactory.consumeProperty(config);
        return value;
    }

    protected synchronized <T> T buildConfigObject(Class<T> configClass, String prefix)
    {
        ConfigurationModule.bindConfig(binder).prefixedWith(prefix).to(configClass);
        return configurationFactory.build(configClass);
    }

    protected synchronized void install(RakamModule module)
    {
        module.setConfigurationFactory(configurationFactory);
        binder.install(module);
    }

    protected abstract void setup(Binder binder);

    public abstract String name();

    public abstract String description();
}