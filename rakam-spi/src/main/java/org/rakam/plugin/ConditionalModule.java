package org.rakam.plugin;

import io.airlift.configuration.ConfigurationFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 18:25.
 */
public interface ConditionalModule {
    boolean shouldInstall(ConfigurationFactory config);
}
