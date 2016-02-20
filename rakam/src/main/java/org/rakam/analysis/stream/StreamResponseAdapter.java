package org.rakam.analysis.stream;

import org.rakam.plugin.stream.StreamResponse;
import org.rakam.server.http.RakamHttpRequest;

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
    public void end() {
        response.end();
    }
}
