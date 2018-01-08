package org.rakam.report;


import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;

public class QueryError {
    public final String message;
    public final String sqlState;
    public final Integer errorCode;
    public final Integer errorLine;
    public final Integer charPositionInLine;

    @JsonCreator
    public QueryError(
            @ApiParam("message") String message,
            @ApiParam(value = "sqlState", required = false) String sqlState,
            @ApiParam(value = "errorCode", required = false) Integer errorCode,
            @ApiParam(value = "errorLine", required = false) Integer errorLine,
            @ApiParam(value = "charPositionInLine", required = false) Integer charPositionInLine) {
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
