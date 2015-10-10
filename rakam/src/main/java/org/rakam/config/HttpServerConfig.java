package org.rakam.config;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;

public class HttpServerConfig {
    private static final int RAKAM_DEFAULT_PORT = 9999;
    private static final String RAKAM_DEFAULT_HOST = "0.0.0.0";

    private HostAndPort address = HostAndPort.fromParts(RAKAM_DEFAULT_HOST, RAKAM_DEFAULT_PORT);
    private boolean disabled;

    @Config("http.server.address")
    public HttpServerConfig setAddress(String address) {
        if(address == null)
            this.address = HostAndPort.fromParts(RAKAM_DEFAULT_HOST, RAKAM_DEFAULT_PORT);
        else
            this.address = HostAndPort.fromString(address).withDefaultPort(RAKAM_DEFAULT_PORT);
        return this;
    }

    public HostAndPort getAddress() {
        return address;
    }

    @Config("http.server.disabled")
    public HttpServerConfig setDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    public boolean getDisabled() {
        return disabled;
    }
}
