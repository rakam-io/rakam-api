package org.rakam.report;

import com.facebook.presto.jdbc.internal.jackson.annotation.JsonCreator;
import com.facebook.presto.jdbc.internal.jackson.annotation.JsonProperty;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 22:32.
 */
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
}
