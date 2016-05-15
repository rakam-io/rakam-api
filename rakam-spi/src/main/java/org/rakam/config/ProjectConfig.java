package org.rakam.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.net.URISyntaxException;

public class ProjectConfig {
    private String lockKey;

    @Config("lock-key")
    @ConfigDescription("A key that is required only for creating projects")
    public ProjectConfig setLockKey(String lockKey) throws URISyntaxException {
        this.lockKey = lockKey;
        return this;
    }

    public String getLockKey() {
        return lockKey;
    }
}
