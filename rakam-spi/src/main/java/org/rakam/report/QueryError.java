package org.rakam.report;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class QueryError {
    public final String message;
    public final String sqlState;
    public final Integer errorCode;
    public final Integer errorLine;
    public final Integer charPositionInLine;

    @JsonCreator
    public QueryError(
            @JsonProperty("message") String message,
            @JsonProperty("sqlState") String sqlState,
            @JsonProperty("errorCode") Integer errorCode,
            @JsonProperty("errorLine") Integer errorLine,
            @JsonProperty("charPositionInLine") Integer charPositionInLine)
    {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.errorLine = errorLine;
        this.charPositionInLine = charPositionInLine;
    }

    public static QueryError create(String message) {
        return new QueryError(message, null, null, null, null);
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
