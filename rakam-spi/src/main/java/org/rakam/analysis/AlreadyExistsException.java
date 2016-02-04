package org.rakam.analysis;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.util.RakamException;

public class AlreadyExistsException extends RakamException {
    public AlreadyExistsException(String itemName, HttpResponseStatus status) {
        super(String.format("%s already exists", itemName), status);
    }
}
