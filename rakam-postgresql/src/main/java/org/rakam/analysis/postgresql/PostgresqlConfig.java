package org.rakam.analysis.postgresql;

import io.airlift.configuration.Config;

public class PostgresqlConfig {

    private boolean autoIndexColumns;

    @Config("postgresql.auto-index-columns")
    public PostgresqlConfig setAutoIndexColumns(boolean indexColumns)
    {
        this.autoIndexColumns = indexColumns;
        return this;
    }

    public boolean isAutoIndexColumns() {
        return autoIndexColumns;
    }
}
