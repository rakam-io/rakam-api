package org.rakam.plugin.user;

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
    private boolean enableUserMapping;
    private String identifierColumn;
    private boolean funnelAnalysisEnabled = true;
    private boolean retentionAnalysisEnabled = true;
    private Iterable<String> actions;
    private boolean automationEnabled;
    private boolean abTestingEnabled;

    @Config("plugin.user.storage.identifier-column")
    public UserPluginConfig setIdentifierColumn(String colName) {
        this.identifierColumn = colName;
        return this;
    }

    @Config("plugin.user.enable-user-mapping")
    public void setEnableUserMapping(boolean enableUserMapping) {
        this.enableUserMapping = enableUserMapping;
    }

    public boolean getEnableUserMapping() {
        return enableUserMapping;
    }

    public String getIdentifierColumn() {
        return identifierColumn;
    }

    @Config("plugin.user.actions")
    public UserPluginConfig setActionList(String actions) {
        this.actions = Splitter.on(",").trimResults().split(actions);
        return this;
    }

    public Iterable<String> getActionList() {
        return actions;
    }

    @Config("user.storage.session-column")
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

    @Config("automation.enabled")
    public void setAutomationEnabled(boolean automationEnabled) {
        this.automationEnabled = automationEnabled;
    }

    public boolean getAutomationEnabled() {
        return automationEnabled;
    }

    @Config("event.ab-testing.enabled")
    public void setAbTestingEnabled(boolean abTestingEnabled) {
        this.abTestingEnabled = abTestingEnabled;
    }
    public boolean getAbTestingEnabled() {
        return abTestingEnabled;
    }

    @Config("user.retention-analysis.enabled")
    public void setRetentionAnalysisEnabled(boolean retentionAnalysisEnabled) {
        this.retentionAnalysisEnabled = retentionAnalysisEnabled;
    }

    public boolean isRetentionAnalysisEnabled() {
        return retentionAnalysisEnabled;
    }

    @Config("plugin.user.storage.hide-columns")
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
