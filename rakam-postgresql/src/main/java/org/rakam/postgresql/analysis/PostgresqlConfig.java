package org.rakam.postgresql.analysis;

import io.airlift.configuration.Config;

public class PostgresqlConfig {

    private boolean autoIndexColumns = true;
    private boolean enableEventStore = true;

    public boolean isAutoIndexColumns() {
        return autoIndexColumns;
    }

    @Config("postgresql.auto-index-columns")
    public PostgresqlConfig setAutoIndexColumns(boolean indexColumns) {
        this.autoIndexColumns = indexColumns;
        return this;
    }
}
