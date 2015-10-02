package org.rakam.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.rakam.report.QueryResult;


public class JsonResponse {
    private static final JsonResponse SUCCESS = new JsonResponse(true);

    public final boolean success;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final String message;

    private JsonResponse(boolean success, String message) {
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
            return JsonResponse.error(queryResult.getError().message);
        }else {
            return JsonResponse.success();
        }
    }
}
