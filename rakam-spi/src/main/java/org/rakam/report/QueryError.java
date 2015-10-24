package org.rakam.report;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class QueryError {
    public final String message;
    public final String sqlState;
    public final int errorCode;
    public final String query;

    @JsonCreator
    public QueryError(
            @JsonProperty("message") String message,
            @JsonProperty("sqlState") String sqlState,
            @JsonProperty("errorCode") int errorCode,
            @JsonProperty("query") String query)
    {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.query = query;
    }

    public QueryError(String message, String sqlState, int errorCode)
    {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.query = null;
    }

    @Override
    public String toString() {
        return "QueryError{" +
                "message='" + message + '\'' +
                ", sqlState='" + sqlState + '\'' +
                ", errorCode=" + errorCode +
                '}';
    }
}
