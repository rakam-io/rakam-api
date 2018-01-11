package org.rakam.aws.dynamodb.apikey;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class DynamodbApiKeyConfig {
    private String tableName;

    @NotNull
    public String getTableName() {
        return tableName;
    }

    @Config("api-key-service.adapter.dynamodb.table")
    public DynamodbApiKeyConfig setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
}
