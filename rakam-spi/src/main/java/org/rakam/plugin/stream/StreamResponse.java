package org.rakam.plugin.stream;


public interface StreamResponse {
    public StreamResponse send(String event, String data);

    public boolean isClosed();

    public void end();
}
