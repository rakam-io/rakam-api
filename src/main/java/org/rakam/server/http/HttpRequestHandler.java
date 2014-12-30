package org.rakam.server.http;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 13:45.
 */
public interface HttpRequestHandler {
    void handle(CustomHttpRequest request);
}
