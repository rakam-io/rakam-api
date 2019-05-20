package org.rakam.presto.analysis;

import io.airlift.configuration.Config;

import java.net.URI;

public class PrestoConfig {
    private URI address;
    private String coldStorageConnector = "rakam_raptor";
    private String checkpointColumn = "_shard_time";


    public URI getAddress() {
        return address;
    }

    @Config("presto.address")
    public PrestoConfig setAddress(URI address) {
        this.address = address;
        return this;
    }

    public String getCheckpointColumn() {
        return checkpointColumn;
    }

    public String getColdStorageConnector() {
        return coldStorageConnector;
    }

    @Config("presto.cold-storage-connector")
    public PrestoConfig setColdStorageConnector(String coldStorageConnector) {
        this.coldStorageConnector = coldStorageConnector;
        return this;
    }
}
