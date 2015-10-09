package org.rakam.plugin;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import java.util.List;


public class UserPluginConfig {
    private String storageModule;
    private List<String> hiddenColumns;
    private boolean mailboxEnabled;
    private String mailBoxStorageModule;
    private String sessionColumn;
    private String lastSeenColumn;
    private String identifierColumn;
    private boolean funnelAnalysisEnabled = true;
    private boolean retentionAnalysisEnabled = true;

    @Config("plugin.user.storage.identifier_column")
    public UserPluginConfig setIdentifierColumn(String colName) {
        this.identifierColumn = colName;
        return this;
    }

    public String getIdentifierColumn() {
        return identifierColumn;
    }

    @Config("user.storage.session_column")
    public void setSessionColumn(String sessionColumn) {
        this.sessionColumn = sessionColumn;
    }

    public String getSessionColumn() {
        return sessionColumn;
    }

    @Config("plugin.user.mailbox.enable")
    public void setMailboxEnabled(boolean mailboxEnabled) {
        this.mailboxEnabled = mailboxEnabled;
    }

    public boolean isMailboxEnabled() {
        return mailboxEnabled;
    }

    @Config("user.funnel-analysis.enabled")
    public void setFunnelAnalysisEnabled(boolean funnelAnalysisEnabled) {
        this.funnelAnalysisEnabled = funnelAnalysisEnabled;
    }

    public boolean isFunnelAnalysisEnabled() {
        return funnelAnalysisEnabled;
    }

    @Config("user.retention-analysis.enabled")
    public void setRetentionAnalysisEnabled(boolean retentionAnalysisEnabled) {
        this.retentionAnalysisEnabled = retentionAnalysisEnabled;
    }

    public boolean isRetentionAnalysisEnabled() {
        return retentionAnalysisEnabled;
    }

    @Config("plugin.user.storage.hide_columns")
    public void setHiddenColumns(String hiddenColumns) {
        this.hiddenColumns = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(hiddenColumns));
    }

    @Config("plugin.user.mailbox.persistence")
    public void setMailBoxStorageModule(String module) {
        this.mailBoxStorageModule = module;
    }

    public String getMailBoxStorageModule() {
        return mailBoxStorageModule;
    }

    public List<String> getHiddenColumns() {
        return hiddenColumns;
    }

    @Config("plugin.user.storage")
    public UserPluginConfig setStorageModule(String moduleName)
    {
        this.storageModule = moduleName;
        return this;
    }

    public String getStorageModule() {
        return storageModule;
    }
}
