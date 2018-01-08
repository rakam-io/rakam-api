package org.rakam.util.javascript;

import org.rakam.analysis.ConfigManager;

import java.util.Optional;

public class JSConfigManager
        implements JSCodeCompiler.IJSConfigManager {
    private final ConfigManager configManager;
    private final String project;
    private final String prefix;

    public JSConfigManager(ConfigManager configManager, String project, String prefix) {
        this.configManager = configManager;
        this.project = project;
        this.prefix = Optional.ofNullable(prefix).map(v -> v + ".").orElse("");
    }

    @Override
    public Object get(String configName) {
        return configManager.getConfig(project, prefix + configName, Object.class);
    }

    @Override
    public void set(String configName, Object value) {
        configManager.setConfig(project, prefix + configName, value);
    }

    @Override
    public Object setOnce(String configName, Object value) {
        return configManager.setConfigOnce(project, prefix + configName, value);
    }
}
