package org.rakam.util;

import org.rakam.server.http.HttpRequestException;


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