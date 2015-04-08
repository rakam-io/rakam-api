package org.rakam.plugin;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 08:24.
 */
public interface StreamResponse {
    public StreamResponse send(String event, String data);

    public boolean isClosed();

    public StreamResponse send(String event, Object data);

    public void end();
}
