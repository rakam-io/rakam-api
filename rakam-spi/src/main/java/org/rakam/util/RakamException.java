package org.rakam.util;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.server.http.HttpRequestException;


public class RakamException extends HttpRequestException {

    public RakamException(String message, HttpResponseStatus statusCode) {
        super(message, statusCode);
    }

    public RakamException(HttpResponseStatus statusCode) {
        super(statusCode.reasonPhrase(), statusCode);
    }
}