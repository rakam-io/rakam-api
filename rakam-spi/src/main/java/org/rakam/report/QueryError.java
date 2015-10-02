package org.rakam.report;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class QueryError {
    public final String message;
    public final String sqlState;
    public final int errorCode;

    @JsonCreator
    public QueryError(
            @JsonProperty("message") String message,
            @JsonProperty("sqlState") String sqlState,
            @JsonProperty("errorCode") int errorCode)
    {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
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
