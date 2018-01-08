package org.rakam.util;


import io.netty.handler.codec.http.HttpResponseStatus;

public class NotExistsException extends RakamException {
    public NotExistsException(String itemName) {
        super(String.format("%s does not exist", itemName), HttpResponseStatus.NOT_FOUND);
    }
}
