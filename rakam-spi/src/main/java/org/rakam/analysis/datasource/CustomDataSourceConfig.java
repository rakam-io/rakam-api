package org.rakam.analysis.datasource;

import io.airlift.configuration.Config;

public class CustomDataSourceConfig
{
    private boolean enabled;

    @Config("custom-data-source.enabled")
    public CustomDataSourceConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public boolean getEnabled()
    {
        return enabled;
    }
}
