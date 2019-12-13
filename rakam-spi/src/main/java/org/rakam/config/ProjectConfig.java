package org.rakam.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.log.Logger;

public class ProjectConfig {
    private String lockKey;
    private String passphrase;
    private String timeColumn = "_time";
    private String clientTimeColumn = "_client_time";
    private String userColumn = "_user";
    private String companyName;
    private int maxStringLength = 100;
    private boolean allowProjectDeletion;

    public String getLockKey() {
        return lockKey;
    }

    @Config("lock-key")
    @ConfigDescription("A key that is required only for creating projects")
    public ProjectConfig setLockKey(String lockKey) {
        this.lockKey = lockKey != null && lockKey.isEmpty() ? null : lockKey;
        return this;
    }

    public String getPassphrase() {
        return passphrase;
    }

    @Config("passphrase")
    public ProjectConfig setPassphrase(String passphrase) {
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

    public String getClientTimeColumn() {
        return clientTimeColumn;
    }

    @Config("client-time-column")
    public ProjectConfig setClientTimeColumn(String timeColumn) {
        this.clientTimeColumn = timeColumn;
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

    public int getMaxStringLength() {
        return maxStringLength;
    }

    @Config("collection.max-string-length")
    public ProjectConfig setMaxStringLength(int maxStringLength) {
        this.maxStringLength = maxStringLength;
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
