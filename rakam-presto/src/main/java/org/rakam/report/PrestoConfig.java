package org.rakam.report;

import io.airlift.configuration.Config;

import java.net.URI;

public class PrestoConfig {
    private URI address;
    private String dataConnectorName;
    private String streamConnectorName;
    private String storage;

    @Config("presto.address")
    public PrestoConfig setAddress(URI address)
    {

        this.address = address;
        return this;
    }

    public String getStorage() {
        return storage;
    }

    @Config("presto.storage")
    public PrestoConfig setStorage(String storage)
    {
        this.storage = storage;
        return this;
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

    public String getColdStorageConnector() {
        return dataConnectorName;
    }

    @Config("presto.hot_storage_connector")
    public PrestoConfig setHotStorageConnector(String streamConnectorName)
    {
        this.streamConnectorName = streamConnectorName;
        return this;
    }

    public String getHotStorageConnector() {
        return streamConnectorName;
    }
}
