package org.rakam.analysis;

import java.util.Optional;

public interface ConfigManager {
    <T> Optional<T> getConfig(String project, String configName, Class<T> clazz);

    <T> void setConfig(String project, String configName, T clazz);

    <T> T setConfigOnce(String project, String configName, T clazz);
}
