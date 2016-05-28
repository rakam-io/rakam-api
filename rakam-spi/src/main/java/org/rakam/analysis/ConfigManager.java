package org.rakam.analysis;

import javax.validation.constraints.NotNull;

public interface ConfigManager {
    <T> T getConfig(String project, String configName, Class<T> clazz);

    <T> void setConfig(String project, String configName, @NotNull T value);

    <T> T setConfigOnce(String project, String configName, @NotNull T clazz);
}
