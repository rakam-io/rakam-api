package org.rakam.analysis;

import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.NotNull;

public interface ConfigManager {
    default void setup() {
    }

    <T> T getConfig(String project, String configName, Class<T> clazz);

    <T> void setConfig(String project, String configName, @NotNull T value);

    <T> T setConfigOnce(String project, String configName, @NotNull T clazz);

    @VisibleForTesting
    void clear();
}
