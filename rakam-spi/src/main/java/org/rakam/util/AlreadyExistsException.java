package org.rakam.util;

import io.netty.handler.codec.http.HttpResponseStatus;

public class AlreadyExistsException extends RakamException {
    public AlreadyExistsException(String itemName, HttpResponseStatus status) {
        super(String.format("%s already exists", itemName), status);
    }
}
