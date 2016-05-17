package org.rakam.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.ApiParam;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;


public class JsonResponse {
    private static final JsonResponse SUCCESS = new JsonResponse(true);

    public final boolean success;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final String message;

    @JsonCreator
    private JsonResponse(@ApiParam("success") boolean success, @ApiParam("message") String message) {
        this.success = success;
        this.message = message;
    }

    private JsonResponse(boolean success) {
        this(success, null);
    }

    public static JsonResponse success() {
        return SUCCESS;
    }

    public static JsonResponse result(boolean success, String message) {
        return new JsonResponse(success, message);
    }

    public static JsonResponse result(boolean success) {
        return new JsonResponse(success, null);
    }

    public static JsonResponse success(String message) {
        return new JsonResponse(true, message);
    }

    public static JsonResponse error(String message) {
        return new JsonResponse(false, message);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JsonResponse)) return false;

        JsonResponse that = (JsonResponse) o;

        if (success != that.success) return false;
        if (message != null ? !message.equals(that.message) : that.message != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (success ? 1 : 0);
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    public static JsonResponse map(QueryResult queryResult) {
        if(queryResult.isFailed()) {
            throw new RakamException(queryResult.getError().message, INTERNAL_SERVER_ERROR);
        } else {
            return JsonResponse.success();
        }
    }
}
