package org.rakam.aws.dynamodb.config;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class DynamodbConfigManagerConfig {
    private String tableName;

    @NotNull
    public String getTableName() {
        return tableName;
    }

    @Config("config-manager.adapter.dynamodb.table")
    public DynamodbConfigManagerConfig setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
}
