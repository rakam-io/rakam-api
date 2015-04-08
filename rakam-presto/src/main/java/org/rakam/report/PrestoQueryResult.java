package org.rakam.report;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.rakam.collection.SchemaField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
* Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:54.
*/
public class PrestoQueryResult implements QueryResult {
    private final List<? extends SchemaField> columns;
    private final List<List<Object>> result;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private final Stat stat;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private Map<String, Object> properties;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private final QueryError error;

    public PrestoQueryResult(List<? extends SchemaField> columns, List<List<Object>> result, QueryError error) {
        this.columns = columns;
        this.result = result;
        this.error = error;
        this.stat = null;
    }

    public PrestoQueryResult(List<? extends SchemaField> columns, List<List<Object>> result, Stat stat, QueryError error) {
        this.columns = columns;
        this.result = result;
        this.error = error;
        this.stat = stat;
    }

    public List<? extends SchemaField> getMetadata() {
        return columns;
    }

    public List<List<Object>> getResult() {
        return result;
    }

    public boolean isFailed() {
        return error != null;
    }

    public QueryError getError() {
        return error;
    }

    public Map<String, Object> setProperty(String key, Object value) {
        if(properties == null) {
            properties = new HashMap<>();
        }

        properties.put(key, value);
        return properties;
    }

    public Stat getStat() {
        return stat;
    }

    public static class Stat {
        public final long totalResult;

        public Stat(long totalResult) {
            this.totalResult = totalResult;
        }
    }
}
