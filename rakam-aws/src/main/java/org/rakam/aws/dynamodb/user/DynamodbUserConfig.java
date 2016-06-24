package org.rakam.aws.dynamodb.user;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class DynamodbUserConfig
{
    private String tableName;

    @Config("plugin.user.storage.dynamodb.table")
    public DynamodbUserConfig setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @NotNull
    public String getTableName()
    {
        return tableName;
    }
}
