package org.rakam.util;

import org.rakam.server.http.HttpRequestException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 27/01/15 22:19.
 */
public class RakamException extends HttpRequestException {
    private final int statusCode;

    public RakamException(String message, int statusCode) {
        super(message, statusCode);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}