package org.rakam.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.log.Logger;

import java.net.URISyntaxException;

public class ProjectConfig {
    private final static Logger LOGGER = Logger.get(ProjectConfig.class);

    private String lockKey;
    private String passphrase;
    private String timeColumn = "_time";
    private String userColumn = "_user";
    private String companyName;
    private boolean allowProjectDeletion;

    public String getLockKey() {
        return lockKey;
    }

    @Config("lock-key")
    @ConfigDescription("A key that is required only for creating projects")
    public ProjectConfig setLockKey(String lockKey) throws URISyntaxException {
        this.lockKey = lockKey != null && lockKey.isEmpty() ? null : lockKey;
        return this;
    }

    public boolean getTasksEnabled() {
        return false;
    }

    @Config("tasks.enable")
    public ProjectConfig setTasksEnabled(boolean tasksEnabled) throws URISyntaxException {
        LOGGER.warn("`tasks.enable` config is deprecated, task feature is not maintained anymore.");
        return this;
    }

    public String getPassphrase() {
        return passphrase;
    }

    @Config("passphrase")
    public ProjectConfig setPassphrase(String passphrase) throws URISyntaxException {
        this.passphrase = passphrase != null && passphrase.isEmpty() ? null : passphrase;
        return this;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    @Config("time-column")
    public ProjectConfig setTimeColumn(String timeColumn) {
        this.timeColumn = timeColumn;
        return this;
    }

    public String getUserColumn() {
        return userColumn;
    }

    @Config("user-column")
    public ProjectConfig setUserColumn(String userColumn) {
        this.userColumn = userColumn;
        return this;
    }

    public String getCompanyName() {
        return companyName;
    }

    @Config("company-name")
    public ProjectConfig setCompanyName(String companyName) {
        this.companyName = companyName;
        return this;
    }

    public boolean getAllowProjectDeletion() {
        return allowProjectDeletion;
    }

    @Config("allow-project-deletion")
    public ProjectConfig setAllowProjectDeletion(boolean allowProjectDeletion) {
        this.allowProjectDeletion = allowProjectDeletion;
        return this;
    }
}
