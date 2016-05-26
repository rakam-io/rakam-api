package org.rakam.analysis;

import java.util.function.Function;

public interface ConfigManager {
    <T> T getConfig(String project, String configName, Class<T> clazz);

    <T> void setConfig(String project, String configName, T clazz);

    <T> void setConfigOnce(String project, String configName, T clazz);

    <T> T computeConfig(String project, String configName, Function<T, T> mapper, Class<T> clazz);
}
