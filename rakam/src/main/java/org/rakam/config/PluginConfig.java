package org.rakam.config;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import java.io.File;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 10/05/15 04:48.
 */
public class PluginConfig {
    private List<String> plugins;
    private File pluginDir = new File(System.getProperty("user.dir"), "plugins");

    @Config("plugin.bundles")
    public PluginConfig setPlugins(String plugins) {
        if (plugins == null) {
            this.plugins = null;
        }
        else {
            this.plugins = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(plugins));
        }
        return this;
    }

    public List<String> getPlugins() {
        return plugins;
    }

    @Config("plugin.directory")
    public PluginConfig setPluginsDirectory(String pluginDir) {
        this.pluginDir = new File(pluginDir);
        return this;
    }

    public File getPluginsDirectory() {
        return pluginDir;
    }
}
