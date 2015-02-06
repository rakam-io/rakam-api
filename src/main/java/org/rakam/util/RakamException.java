package org.rakam.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/01/15 22:19.
 */
public class RakamException extends RuntimeException {
    private final int statusCode;

    public RakamException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}