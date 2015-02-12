package org.rakam.plugin;

import io.airlift.configuration.AbstractConfigurationAwareModule;

/**
 * Created by buremba on 29/03/14.
 */
public abstract class RakamModule extends AbstractConfigurationAwareModule {
    public abstract String name();

    public abstract String description();

    public abstract void onDestroy();
}