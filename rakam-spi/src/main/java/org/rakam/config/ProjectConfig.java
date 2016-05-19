package org.rakam.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.net.URISyntaxException;

public class ProjectConfig {
    private String lockKey;
    private String passphrase;

    @Config("lock-key")
    @ConfigDescription("A key that is required only for creating projects")
    public ProjectConfig setLockKey(String lockKey) throws URISyntaxException {
        this.lockKey = lockKey;
        return this;
    }

    public String getLockKey() {
        return lockKey;
    }

    @Config("passphrase")
    public ProjectConfig setPassphrase(String passphrase) throws URISyntaxException {
        this.passphrase = passphrase;
        return this;
    }

    public String getPassphrase() {
        return passphrase;
    }
}
