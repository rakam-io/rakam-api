package org.rakam.report;

import io.airlift.configuration.Config;

import java.net.URI;

public class PrestoConfig {
    private URI address;
    private String dataConnectorName;
    private String hotStorageConnectorName;
    private String streamingConnector = "streaming";
    private String userConnector = "user";

    @Config("presto.address")
    public PrestoConfig setAddress(URI address)
    {

        this.address = address;
        return this;
    }

    @Config("presto.user_connector")
    public PrestoConfig setUserConnector(String userConnector)
    {

        this.userConnector = userConnector;
        return this;
    }

    public String getUserConnector() {
        return userConnector;
    }

    public URI getAddress() {
        return address;
    }

    @Config("presto.cold_storage_connector")
    public PrestoConfig setColdStorageConnector(String connectorName)
    {
        this.dataConnectorName = connectorName;
        return this;
    }

    @Config("presto.streaming_connector")
    public PrestoConfig setStreamingConnector(String connectorName)
    {
        this.streamingConnector = connectorName;
        return this;
    }

    public String getStreamingConnector() {
        return streamingConnector;
    }

    public String getColdStorageConnector() {
        return dataConnectorName;
    }

    @Config("presto.hot_storage_connector")
    public PrestoConfig setHotStorageConnector(String streamConnectorName)
    {
        this.hotStorageConnectorName = streamConnectorName;
        return this;
    }

    public String getHotStorageConnector() {
        return hotStorageConnectorName;
    }
}
