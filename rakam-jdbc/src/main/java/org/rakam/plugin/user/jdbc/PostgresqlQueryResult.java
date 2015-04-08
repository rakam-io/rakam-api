package org.rakam.plugin.user.jdbc;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryError;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;

/**
* Created by buremba <Burak Emre Kabakcı> on 04/04/15 17:48.
*/
class PostgresqlQueryResult implements QueryResult {
    private final List<? extends SchemaField> columns;
    private final List<List<Object>> result;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private Map<String, Object> properties;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final QueryError error;

    public PostgresqlQueryResult(QueryError error, Map<String, Object> properties, List<List<Object>> result, List<? extends SchemaField> columns) {
        this.error = error;
        this.properties = properties;
        this.result = result;
        this.columns = columns;
    }

    @Override
    public QueryError getError() {
        return error;
    }

    @Override
    public boolean isFailed() {
        return error != null;
    }

    @Override
    public List<List<Object>> getResult() {
        return result;
    }

    @Override
    public List<? extends SchemaField> getMetadata() {
        return columns;
    }

    @Override
    public String toString() {
        return "PostgresqlQueryResult{" +
                "columns=" + columns +
                ", result=" + result +
                ", properties=" + properties +
                ", error=" + error +
                '}';
    }
}

