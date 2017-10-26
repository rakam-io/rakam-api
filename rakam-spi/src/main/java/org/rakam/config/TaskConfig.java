package org.rakam.config;

import io.airlift.configuration.Config;

public class TaskConfig {
    public boolean tasksEnabled;
    private boolean webhookEnabled;

    public boolean getTasksEnabled() {
        return tasksEnabled;
    }

    public boolean getWebhookEnabled() {
        return webhookEnabled;
    }

    @Config("tasks.enable")
    public TaskConfig setTasksEnabled(boolean enabled) {
        this.tasksEnabled = enabled;
        return this;
    }

    @Config("webhook.enable")
    public TaskConfig setWebhookEnabled(boolean enabled) {
        this.webhookEnabled = enabled;
        return this;
    }
}
