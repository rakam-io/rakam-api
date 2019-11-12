package org.rakam.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.rakam.server.http.annotations.ApiParam;

import java.util.Objects;


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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SuccessMessage)) return false;

        SuccessMessage that = (SuccessMessage) o;

        if (!Objects.equals(message, that.message)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return message == null ? 1 : message.hashCode();
    }
}
