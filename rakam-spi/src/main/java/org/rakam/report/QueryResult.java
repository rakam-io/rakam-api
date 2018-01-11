package org.rakam.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.swagger.annotations.ApiModelProperty;
import org.rakam.collection.SchemaField;
import org.rakam.server.http.annotations.ApiParam;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryResult {
    public static final String EXECUTION_TIME = "executionTimeInMillis";
    public static final String QUERY = "query";
    public static final String TOTAL_RESULT = "totalResult";
    private static final QueryResult EMPTY = new QueryResult(ImmutableList.of(), ImmutableList.of());
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final List<SchemaField> metadata;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final List<List<Object>> result;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private final QueryError error;
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private Map<String, Object> properties;

    @JsonCreator
    private QueryResult(
            @ApiParam("metadata") List<SchemaField> metadata,
            @ApiParam("result") List<List<Object>> result,
            @ApiParam("error") QueryError error,
            @ApiParam("properties") Map<String, Object> properties) {
        this.metadata = metadata;
        this.result = result;
        this.error = error;
        this.properties = properties;
    }
    public QueryResult(List<SchemaField> metadata, List<List<Object>> result) {
        this(metadata, result, null, null);
    }

    public QueryResult(List<SchemaField> metadata, List<List<Object>> result, Map<String, Object> properties) {
        this(metadata, result, null, properties);
    }

    public static QueryResult errorResult(QueryError error) {
        return new QueryResult(null, null, error, null);
    }

    public static QueryResult errorResult(QueryError error, String query) {
        return new QueryResult(null, null, error, ImmutableMap.of("query", query));
    }

    public static QueryResult empty() {
        return EMPTY;
    }

    public QueryError getError() {
        return error;
    }

    public Map<String, Object> getProperties() {
        return properties == null ? ImmutableMap.of() : properties;
    }

    public synchronized void setProperty(String key, Object value) {
        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();
        if (properties != null) {
            map.putAll(properties);
        }
        map.put(key, value);
        properties = map;
    }

    public boolean isFailed() {
        return error != null;
    }

    @ApiModelProperty(value = "Each row is an array that contains the values for the columns that are defined in metadata property.")
    public List<List<Object>> getResult() {
        return result;
    }

    public List<SchemaField> getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                (error == null ? "" : "error=" + error) +
                ", result=" + (result == null ? "" : Joiner.on(", ").join(result)) +
                ", metadata=" + (metadata == null ? "" : metadata) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryResult)) {
            return false;
        }

        QueryResult result1 = (QueryResult) o;

        if (error != null ? !error.equals(result1.error) : result1.error != null) {
            return false;
        }
        if (metadata != null ? !metadata.equals(result1.metadata) : result1.metadata != null) {
            return false;
        }
        if (properties != null ? !properties.equals(result1.properties) : result1.properties != null) {
            return false;
        }
        if (result != null ? !result.equals(result1.result) : result1.result != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result1 = metadata != null ? metadata.hashCode() : 0;
        result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
        result1 = 31 * result1 + (error != null ? error.hashCode() : 0);
        result1 = 31 * result1 + (properties != null ? properties.hashCode() : 0);
        return result1;
    }
}
