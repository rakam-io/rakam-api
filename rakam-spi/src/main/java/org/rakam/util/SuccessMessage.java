package org.rakam.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.ApiParam;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;


public class SuccessMessage {
    private static final SuccessMessage SUCCESS = new SuccessMessage(null);

    public final boolean success = true;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final String message;

    @JsonCreator
    private SuccessMessage(@ApiParam("message") String message) {
        this.message = message;
    }

    public static SuccessMessage success() {
        return SUCCESS;
    }

    public static SuccessMessage success(String message) {
        return new SuccessMessage(message);
    }

    public static SuccessMessage map(QueryResult queryResult) {
        if (queryResult.isFailed()) {
            throw new RakamException(queryResult.getError().message, INTERNAL_SERVER_ERROR);
        } else {
            return SuccessMessage.success();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SuccessMessage)) return false;

        SuccessMessage that = (SuccessMessage) o;

        if (message != null ? !message.equals(that.message) : that.message != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return message == null ? 1 : message.hashCode();
    }
}
