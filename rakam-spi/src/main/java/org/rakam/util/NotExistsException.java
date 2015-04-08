package org.rakam.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 04:50.
 */
public class NotExistsException extends RuntimeException {
    public NotExistsException(String message) {
        super(message);
    }

    public NotExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotExistsException(Throwable cause) {
        super(cause);
    }
}
