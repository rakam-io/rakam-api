package org.rakam.analysis;

import org.rakam.plugin.StreamResponse;
import org.rakam.server.http.RakamHttpRequest;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 08:30.
 */
public class StreamResponseAdapter implements StreamResponse {
    private final RakamHttpRequest.StreamResponse response;

    public StreamResponseAdapter(RakamHttpRequest.StreamResponse response) {
        this.response = response;
    }

    @Override
    public StreamResponse send(String event, String data) {
        response.send(event, data);
        return this;
    }

    @Override
    public boolean isClosed() {
        return response.isClosed();
    }

    @Override
    public StreamResponse send(String event, Object data) {
        response.send(event, data);
        return this;
    }

    @Override
    public void end() {
        response.end();
    }
}
