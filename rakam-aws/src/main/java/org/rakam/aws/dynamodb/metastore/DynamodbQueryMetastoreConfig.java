package org.rakam.aws.dynamodb.metastore;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class DynamodbQueryMetastoreConfig {
    private String tableName;

    @NotNull
    public String getTableName() {
        return tableName;
    }

    @Config("query-metadata-store.adapter.dynamodb.table")
    public DynamodbQueryMetastoreConfig setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
}
