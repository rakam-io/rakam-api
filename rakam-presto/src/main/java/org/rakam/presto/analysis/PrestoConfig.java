package org.rakam.presto.analysis;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import java.net.URI;
import java.util.List;

public class PrestoConfig {
    private URI address;
    private String coldStorageConnector = "rakam_raptor";
    private String hotStorageConnector;
    private String streamingConnector = "streaming";
    private String userConnector = "user";
    private String bulkConnector = "middleware";
    private String checkpointColumn = "_shard_time";
    private List<String> existingProjects;

    @Config("presto.address")
    public PrestoConfig setAddress(URI address)
    {

        this.address = address;
        return this;
    }

    @Config("presto.user-connector")
    public PrestoConfig setUserConnector(String userConnector)
    {

        this.userConnector = userConnector;
        return this;
    }

    @Config("presto.existing-schemas")
    public PrestoConfig setExistingProjects(String existingProjects)
    {
        this.existingProjects = existingProjects != null ?
                ImmutableList.copyOf(Splitter.on(",").trimResults()
                        .split(existingProjects)) : null;
        return this;
    }

    public List<String> getExistingProjects()
    {
        return existingProjects;
    }

    @Config("presto.bulk-connector")
    public PrestoConfig setBulkConnector(String bulkConnector)
    {

        this.bulkConnector = bulkConnector;
        return this;
    }

    public String getUserConnector() {
        return userConnector;
    }

    public URI getAddress() {
        return address;
    }

    @Config("presto.cold-storage-connector")
    public PrestoConfig setColdStorageConnector(String coldStorageConnector)
    {
        this.coldStorageConnector = coldStorageConnector;
        return this;
    }

    @Config("presto.streaming-connector")
    public PrestoConfig setStreamingConnector(String connectorName)
    {
        this.streamingConnector = connectorName;
        return this;
    }

    @Config("presto.checkpoint-column")
    public PrestoConfig setCheckpointColumn(String checkpointColumn)
    {
        this.checkpointColumn = checkpointColumn;
        return this;
    }

    public String getCheckpointColumn()
    {
        return checkpointColumn;
    }

    public String getStreamingConnector() {
        return streamingConnector;
    }

    public String getColdStorageConnector() {
        return coldStorageConnector;
    }

    @Config("presto.hot-storage-connector")
    public PrestoConfig setHotStorageConnector(String streamConnectorName)
    {
        this.hotStorageConnector = streamConnectorName;
        return this;
    }

    public String getHotStorageConnector() {
        return hotStorageConnector;
    }

    public String getBulkConnector() {
        return bulkConnector;
    }
}
